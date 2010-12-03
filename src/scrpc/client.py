"""This file contains the single-minded RPC client."""

from __future__ import with_statement
from timedsocket import TimedSocket
from cPickle import loads, dumps, PicklingError, UnpicklingError
from functools import partial
from thread import allocate_lock
import logging

class SCProxy(object):
    """A proxy handling a connection to an instance of SCRPC"""

    MAX_CALL_LENGTH = 600.0 # The maximum number of seconds to wait for a remote call to finish.

    class RemoteError(Exception):
        def __init__(self, *args):
            super(SCProxy.SCProxy.RemoteError, self).__init__(*args)
    
    class CommunicationError(Exception):
        def __init__ (self, *args):
            super(SCProxy.SCProxy.CommunicationError , self).__init__(*args)
    
    class MarshalingError(Exception):
        def __init__(self, *args):
            super(SCProxy.SCProxy.MarshalingError, self).__init__(*args)
    
    def __init__(self, address=('localhost', 3344)):
        super(SCProxy, self).__init__()
        
        # Store member variables.
        self.__address = address

        # Create a socket and connect to the server.
        self.__sock = TimedSocket()
        self.__connected = False
        self.__connect()     
        self.__lock = allocate_lock()       
    
    def __nonzero__(self):
        return True
    
    def __getattr__(self, attrname):
        """
        Forwards any unknown attribute requests to the function that 
        makes RPC calls to the server.
        @type attrname: str
        @param attrname: The attribute to search for. In this case it must 
        be the name of a remote method on the RPC server. 
        """
        if attrname == '':
            return self
        else:
            return partial(self.make_rpc_call, attrname)
    
    def set_address(self, address):
        """
        Set the address of the RPC server.
        @type address: tuple
        @param address: The address where the SCRPC server is
        listening.
        """
        self.__address = address
    
    def __connect(self):
        """Connects to an instance of SCRPC."""
        try:
            self.__sock.connect(self.__address)
        except Exception, excep:
            raise SCProxy.CommunicationError('Error connecting to RPC server.', excep)
        self.__connected = True
        
    def __disconnect(self, quiet=False):
        """Disconnects from the server."""
        try:
            self.__sock.close()
        except Exception, excep:
            if not quiet:
                raise SCProxy.CommunicationError('Error disconnecting from server.', excep)
        self.__connected = False
    
    def close(self):
        """Publicly available disconnect method."""
        self.__disconnect()
    
    def make_rpc_call(self, function_name, *function_input):
        """
        Perform a remote procedure call.
        @type function_name: str
        @param function_name: The name of the remote function to call.
        @type function_input: list
        @param function_input: The input for the remote function.
        """
        logger = logging.getLogger("SMRPC-Client")
        
        with self.__lock:
            # Make sure that the proxy is connected to the server.
            if not self.__connected:
                self.__connect()
            
            # Marshal the function input.
            try:
                marshalled_input = dumps(function_input, -1)
            except PicklingError, excep:
                raise SCProxy.MarshalingError('Error marshaling function input', excep)
            
            # Send the PERFORM request to the server.
            try:
                self.__sock.send_lp('PERFORM %s'%function_name)
            except Exception, excep:
                logger.info('Error sending PERFORM request to server.', exc_info=True)
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Error sending PERFORM request to server.', excep)
            
            # Wait for the server to acknowledge the PERFORM request.
            try:
                response = self.__sock.recv_lp()
            except Exception, excep:
                logger.info('Server did not respond to PERFORM request.', exc_info=True)
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Server did not respond to PERFORM request.', excep)
            
            # Check the response from the server.
            if response == '':
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Connection closed by server.')
            elif response[:4] == 'NACK':
                raise SCProxy.RemoteError('%s'%response[5:])
            
            # Now send the input to the function call.
            try:
                self.__sock.send_lp(marshalled_input)
            except Exception, excep:
                logger.info('Error sending function input to server.', exc_info=True)
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Error sending function input to server.', excep)
            
            # Wait for the server to acknowledge the receipt of the input.
            try:
                response = self.__sock.recv_lp()
            except Exception, excep:
                logger.info('Server did not ack receipt of input.', exc_info=True)
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Server did not ack receipt of input.', excep)
            
            # Check the response.
            if response == '':
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Connection closed by server.')
            elif response[:4] == 'NACK':
                raise SCProxy.RemoteError('%s'%response[5:])
            elif response[:9] == 'EXCEPTION':
                try:
                    raise loads(response[10:])
                except (UnpicklingError, ImportError), excep:
                    raise SCProxy.RemoteError('Unknown exception raised on server.', excep)
            
            # The input has successfully arrived at the server. Now wait for the 
            # result - or an error indication.
            try:
                result = self.__sock.recv_lp(SCProxy.MAX_CALL_LENGTH)
            except (TimedSocket.Timeout, TimedSocket.Exception), excep:
                raise SCProxy.RemoteError('Timeout while performing remote function.', excep)
            except Exception, excep:
                logger.info('Error receiving remote function output.', exc_info=True)
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Error receiving remote function output.', excep)
            
            # Check the result.
            if result == '':
                self.__disconnect(True)
                raise SCProxy.CommunicationError('Connection closed by server.')
            elif result[:9] == 'EXCEPTION':
                try:
                    raise loads(result[10:])
                except (UnpicklingError, ImportError), excep:
                    raise SCProxy.RemoteError('Unknown exception raised on server.', excep)
            
            # Return result.
            try:
                return loads(result[7:])
            except (UnpicklingError, ImportError), excep:
                raise SCProxy.MarshalingError('Error unmarshalling result.', excep)
            
