"""This file contains the single-minded RPC server. This is a 
single-threaded, persistent (as in holds a persistent TCP connection
between the client and the server) RPC server."""

from __future__ import with_statement
from timedsocket import TimedSocket
from threading import Thread
from types import FunctionType, StringType, TupleType, MethodType
from cPickle import loads, dumps, UnpicklingError, PicklingError
from thread import allocate_lock
import logging

class SCWorker(Thread):
    POLL_PERIOD = 1.0

    def __init__(self, sock, rpcserver):
        super(SCWorker, self).__init__()
        self.__client_sock = sock
        self.__server = rpcserver

    def disconnect_client(self):
        """Closes the current client connection."""
        try:
            self.__server.remove_connection(self)
            self.__client_sock.close()
        except:
            pass

    def run(self):
        """Waits for a request from the client to arrive."""
        logger = logging.getLogger('SCRPC (server)')
        logger.debug('SCRPC worker spawned, port=%i.' % self.__client_sock.addr[1])
        while not self.__server.shutdown():
            # Wait for a command to arrive from the client.
            try:
                cmd = self.__client_sock.recv_lp(SCWorker.POLL_PERIOD)
                if cmd == '':
                    # The connection has been closed.
                    logger.debug('Connection closed by peer.')
                    break
            except TimedSocket.Timeout:
                continue
            except TimedSocket.Exception:
                # The connection is probably broken.
                logger.debug('Connection broken', exc_info=True)
                break
        
            # A command has arrived. Check what it is!
            if cmd[:7] == 'PERFORM':
                try:
                    if self.__perform_rpc(cmd[8:]) == False:
                        # An error occurred - close down the thread.
                        logger.debug('Error performing RPC.')
                        break
                except:
                    logger.debug('Unhandled exception while performing RPC.', exc_info=True)
                    break

        # Shutdown.
        logger.debug('SCRPC worker leaving, port=%i.' % self.__client_sock.addr[1])
        self.disconnect_client()
                    
    def __perform_rpc(self, function_name):
        """
        Performs an RPC call.
        @type function_name: function
        @param function_name: The function to call.
        """
        logger = logging.getLogger('SCRPC (server)') 

        # Check that the function exists.
        function = None
        try:
            function = self.__server.get_function(function_name)
            if function != None:
                self.__client_sock.send_lp('ACK')
            else:
                self.__client_sock.send_lp('NACK Function (%s) does not exist.' % function_name)
                return True
        except (TimedSocket.Timeout, TimedSocket.Exception):
            # The connection has somehow failed.
            logger.debug('perform_rpc(1)', exc_info=True)
            self.disconnect_client()
            return False

        # <HACK> WARNING!!! 
        # The following is a scavenger specific hack that signals the intent to call 
        # a function on the RPC server. If you have registered a function called 
        # the same name as the one being called with '_intent' appended, this function
        # will be called now to show the intent of the client to call the function.
        # The argument to the intent function is a boolean signaling failure. In 
        # this initial case we only know of the intent and there is no error.
        intent_function = self.__server.get_function('%s_intent' % function_name)
        if intent_function != None: intent_function(False)
        # </HACK>

        # The function exists - wait for the client to send the input.
        try:
            cmd_input = self.__client_sock.recv_lp()
            if cmd_input == '':
                # The connection has been closed.
                logger.debug('Client did not send input')
                self.disconnect_client()
                return False
        except (TimedSocket.Timeout, TimedSocket.Exception):
            # The connection is probably broken.
            logger.debug('perform_rpc(2)', exc_info=True)
            # <HACK>
            if intent_function: intent_function(True)
            # </HACK>
            self.disconnect_client()
            return False
        
        # The command input has been received. Unmarshal it.
        try:
            argument_list = loads(cmd_input)
        except (UnpicklingError, ImportError), excep:
            # An error occurred unmarshalling the input. Return the 
            # exception to the caller.
            logger.debug('Unpickling error', exc_info=True)
            # <HACK>
            if intent_function: intent_function(True)
            # </HACK>
            try:
                self.__client_sock.send_lp('EXCEPTION %s' % dumps(excep, -1))
                return True
            except (TimedSocket.Timeout, TimedSocket.Exception):
                # The connection is probably broken.
                logger.debug('perform_rpc(3)', exc_info=True)
                self.disconnect_client()
                return False
        
        # Do simple type checking.
        try:
            if type(argument_list) != TupleType:
                self.__client_sock.send_lp('NACK Argument must be a tuple.')
                # <HACK>
                if intent_function: intent_function(True)
                # </HACK>
                return True
            else:
                self.__client_sock.send_lp('ACK')
        except (TimedSocket.Timeout, TimedSocket.Exception):
            # The connection is probably broken.
            logger.debug('perform_rpc(4)', exc_info=True)
            # <HACK>
            if intent_function: intent_function(True)
            # </HACK>
            self.disconnect_client()
            return False
        
        # Call the RPC function.
        try:
            cmd_output = function(*argument_list) #IGNORE:W0142
        except Exception, excep: #IGNORE:W0703
            # An exception occurred executing the function. Send the 
            # exception back to the caller.
            try:
                self.__client_sock.send_lp('EXCEPTION %s' % dumps(excep, -1))
                return True
            except (TimedSocket.Timeout, TimedSocket.Exception), excep:
                # The connection is probably broken.
                logger.debug('perform_rpc(5)', exc_info=True)
                self.disconnect_client()
                return False
           
        # The command has been successfully executed. Now return the 
        # output to the caller.
        # Start by marshaling it.
        try:
            marshalled_result = dumps(cmd_output, -1)
        except PicklingError, excep:
            # The result could not be marshaled. Return the exception to 
            # the caller.
            logger.debug('Pickling error', exc_info=True)
            try:
                self.__client_sock.send_lp('EXCEPTION %s' % dumps(excep, -1))
                return True
            except (TimedSocket.Timeout, TimedSocket.Exception):
                # The connection is probably broken.
                logger.debug('perform_rpc(6)', exc_info=True)
                self.disconnect_client()
                return False
            
        # Send the marshaled result to the caller.
        try:
            self.__client_sock.send_lp('RESULT %s' % marshalled_result)
        except (TimedSocket.Timeout, TimedSocket.Exception):
            # The connection is probably broken.
            logger.debug('perform_rpc(7)', exc_info=True)
            self.disconnect_client()
            return False
        
        # Everything was successfully performed...
        return True

class SCRPC(Thread):
    """The single-connection RPC server."""
    
    class Error(Exception):
        """
        The exception type raised when an error occurs within the 
        SCRPC server.
        """
        def __init__(self, msg):
            super(SCRPC.Error, self).__init__(msg)

    def __init__(self, address=('', 0)):
        """
        Constructor.
        @type address: tuple
        @param address: The address that the RPC server should listen on for
        incoming connection requests. 
        """
        # Initialize super class.
        super(SCRPC, self).__init__()
        
        # Create server socket listening for incoming requests.
        self.__server_sock = TimedSocket()
        self.__server_sock.bind(address)
        self.__server_sock.listen(5)
        
        # Set member variables.
        self.__functions = {}
        self.__shutdown = False
        self.__shutdown_signal = allocate_lock()

    def shutdown(self):
        return self.__shutdown

    def get_function(self, function_name):
        if self.__functions.has_key(function_name):
            return self.__functions[function_name]
        else:
            return None
    
    def remove_connection(self, connection):
        with self.__connections_lock:
            self.__connections.remove(connection)
    
    def add_connection(self, connection):
        with self.__connections_lock:
            self.__connections.append(connection)
        
    def get_address(self):
        return self.__server_sock.addr
        
    def debug_print(self):
        print 'Registered functions:', self.__functions

    def register_function(self, rpc_function, rpc_name=''):
        """
        Registers a new function with the RPC server. This function may 
        afterwards be called by remote clients.
        @type rpc_function: function
        @param rpc_function: The function to register.
        @type rpc_name: str
        @param rpc_name: An (optional) name to use for the function. If this is not
        given the functions original name is used.
        """
        # Do simple type-checking.
        if not type(rpc_function) in (FunctionType, MethodType) or type(rpc_name) != StringType:
            raise TypeError('Arguments of invalid type given.')
        
        # Check that the name is not already taken.
        if rpc_name == '':
            rpc_name = rpc_function.__name__
        if self.__functions.has_key(rpc_name):
            raise SCRPC.Error('The function name %s is already taken.' % rpc_name)
        
        # Add the function to the list.
        self.__functions[rpc_name] = rpc_function
            
    def stop(self, block=False):
        """Stop the RPC server thread.
        @type block: bool
        @param block: Whether or not the call should block until the service is 
        closed down properly.
        """
        self.__shutdown = True
        
        if block:
            self.__shutdown_signal.acquire()
            self.__shutdown_signal.release()

    def teardown(self):
        """
        Close down the RPC server completely. This closes the server socket
        making the SCRPC object unusable.
        """
        self.__server_sock.close()

    def run(self):
        """
        Main thread function.
        """
        self.__shutdown_signal.acquire()
        self.__shutdown = False
        
        while not self.__shutdown:
        # Wait for an incoming connection attempt.
            try:
                # Accept an incoming connection attempt.
                new_sock = self.__server_sock.accept()
                # Create a new worker thread and start it.
                connection = SCWorker(new_sock, self)
                connection.start()
            except TimedSocket.Timeout:
                continue
                    
        # Reset the shutdown indicator.
        self.__shutdown_signal.release()

    def __wait_for_connection(self):
        """Waits for a connection to arrive on the server socket."""
        # Wait for an incoming connection attempt.
        try:
            connection = self.__server_sock.accept()
        except TimedSocket.Timeout:
            return

        # A connection has been established.
        self.__connected = True
        self.__client_sock = connection

        
