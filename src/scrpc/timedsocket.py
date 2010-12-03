"""
A socket wrapper that offers timed (semi-blocking) calls
to all required socket functions. Essentially this is just 
a socket class using select() to see if anything is available 
_before_ entering the blocking socket calls.
"""

from socket import socket, SOCK_DGRAM, SOCK_STREAM, AF_INET
from select import select
import struct

class TimedSocket(object):
    """
    The timed socket class. This class wraps a native Python socket
    adding timeouts to all blocking calls.
    """

    class Exception(Exception):
        """Exception thrown by the TimesSocket class."""
        def __init__(self, msg):
            super(TimedSocket.Exception, self).__init__(msg)
    
    class Timeout(Exception):
        """Exception used to signal timeout."""
        def __init__(self):
            super(TimedSocket.Timeout, self).__init__('Operation timed out.')

    # Constants.
    TIMEOUT = 10
    LP_RECV_CHUNK_TIMEOUT = 60

    def __init__(self, **args):
        """
        Constructor.
        @type args: Keyword argument(s).
        @param args: Keyword args are accepted. Allowed keywords are 'type' and
        'timeout'. 'type' designates the type of socket (udp or tcp) and 'timeout'
        sets the default timeout period for blocking calls.
        @raise ValueError: If the timeout period or the socket type is invalid.
        """
        super(TimedSocket, self).__init__()

        # Set socket type.
        if args.has_key('type'):
            self.socket_type = args['type']
        else:
            self.socket_type = 'tcp'

        # Check whether the 'sock' key exists in the arguments. If so we 
        # just wrap the given socket object.
        if args.has_key('wrap'):
            self.sock, self.addr = args['wrap']
        else:         
            # Create a new native socket.
            if self.socket_type == 'tcp':
                self.sock = socket(AF_INET, SOCK_STREAM, 0)
            elif self.socket_type == 'udp':
                self.sock = socket(AF_INET, SOCK_DGRAM, 0)
            else:
                raise ValueError('Unknown socket type (%s)'%self.socket_type)
            self.addr = None
        
        # Set the timeout period.
        if args.has_key('timeout'):
            self.timeout = args['timeout']
            # Sanity check.
            if self.timeout < 0:
                raise ValueError('Invalid timeout period (%f)'%self.timeout)
        else:
            self.timeout = TimedSocket.TIMEOUT
    
    def bind(self, address):
        """
        Direct wrapper of the bind function of a native socket.
        @see: socket.bind
        """
        self.sock.bind(address)
        self.addr = self.sock.getsockname()
        
    def listen(self, backlog):
        """
        Direct wrapper of the listen function of a native socket.
        @see: socket.listen
        """
        self.sock.listen(backlog)

    def accept(self, timeout=None):
        """
        Semi-blocking accept call.
        @type timeout: float
        @param timeout: A custom timeout period used for this one function call.
        @rtype: tuple
        @return: The address of the connected peer and a TimedSocket object 
        containing the connection. 
        @raise Timeout: If the timeout is reached.
        @raise ValueError: If the timeout value is invalid.
        @see: socket.accept
        """
        # Set the timeout used for this command and start listening for 
        # activity.
        if not timeout:
            timeout = self.timeout
        else:
            # Sanity check.
            if timeout < 0:
                raise ValueError('Invalid timeout period (%f)'%timeout)
        (readers, _, exceptions) = select([self.sock], [], [self.sock], timeout)
        
        # Check for exceptions - this probably means something is wrong.
        if len(exceptions) != 0:
            raise TimedSocket.Exception('Connection broken?')
        
        # Check for activity on the socket.
        if len(readers) != 0:
            connection = self.sock.accept()
            return TimedSocket(type='tcp', wrap=connection)
        # No activity means that the timeout was reached.
        else:
            raise TimedSocket.Timeout()
    
    def connect(self, address):
        """
        Wrapper of the native connect call.
        @see: socket.connect
        """
        self.sock.connect(address)
    
    def send(self, message, timeout=None):
        """
        Semi-blocking send call.
        @type message: str
        @param message: The message to send.
        @type timeout: float
        @param timeout: A custom timeout period used for this one function call.
        @rtype: int
        @return: The number of bytes successfully sent to the peer.
        @raise Timeout: If the timeout is reached before sending has commenced.
        @raise ValueError: If the timeout value is invalid.
        @see: socket.send
        """
        # Set the timeout used for this command and start listening.
        if not timeout:
            timeout = self.timeout
        else:
            # Sanity check.
            if timeout < 0:
                raise ValueError('Invalid timeout period (%f)'%timeout)

        (_, writers, exceptions) = select([], [self.sock], [self.sock], timeout)
        
        # Check for exceptions - this probably means something is wrong.
        if len(exceptions) != 0:
            raise TimedSocket.Exception('Connection broken?')

        # Check to see if the socket is ready to receive data.
        if len(writers) != 0:
            return self.sock.send(message)
        else:
            # The timeout must have been reached.
            raise TimedSocket.Timeout()
    
    def recv(self, buffersize, timeout=None):
        """
        Semi-blocking receive call.
        @type buffersize: int
        @param buffersize: The maximum number of bytes to read from the socket.
        @type timeout: float
        @param timeout: A custom timeout period used for this one function call.
        @rtype: str
        @return: The message that was read from the socket.
        @raise Timeout: If the timeout is reached before data arrives.
        @raise ValueError: If the timeout value is invalid.
        @raise TimedSocket.Exception: If an error occurs on the native socket.
        @see: socket.recv
        """
        # Set the timeout used for this command and start listening for 
        # activity.
        if not timeout:
            timeout = self.timeout
        else:
            # Sanity check.
            if timeout < 0:
                raise ValueError('Invalid timeout period (%f)'%timeout)
        
        (readers, _, exceptions) = select([self.sock], [], [self.sock], timeout)
        
        # Check for exceptions - this probably means something is wrong.
        if len(exceptions) != 0:
            raise TimedSocket.Exception('Connection broken?')
        
        # Check for activity on the socket.
        if len(readers) != 0:
            return self.sock.recv(buffersize)
        else:
            # The command timed out.
            raise TimedSocket.Timeout()
    
    def send_lp(self, msg, timeout=None):
        """
        A length-prefixed send method. This method simply prefixes the length 
        of the message as an unsigned int (4 bytes) before sending the message.
        @see: TimedSocket.send
        """
        #print 'send ->', msg #DEBUG
        message = struct.pack('!I', len(msg)) + msg
        length = len(message)
        sent = 0
        while sent < length:
            sent += self.send(message[sent:sent+4096], timeout)
                
    def recv_lp(self, timeout=None):
        """
        A length-prefixed version of the recv call. This method assumes that 
        messages are prefixed by an unsigned int designating the length of 
        the message.
        @raise TimedSocket.Exception: If the lp format is incorrect.
        @see: TimedSocket.recv
        """
        # Wait for the message length indicator.
        try:
            msg = self.recv(4, timeout)
            if msg == '':
                # The connection has been closed.
                return msg
            (msg_length, ) = struct.unpack("!I", msg)
        except struct.error:
            raise TimedSocket.Exception('Invalid format for lp-mode.')
    
        # The message length has been read. Now receive the specified
        # amount of bytes. 
        timeout = TimedSocket.LP_RECV_CHUNK_TIMEOUT
        timed_out = False
        chunk_table = []
        received_so_far = 0
        while received_so_far < msg_length:
            try:
                next_chunk_size = min(4096, msg_length - received_so_far) 
                chunk = self.recv(next_chunk_size, timeout)
                if chunk == '':
                    # The connection has been closed.
                    raise TimedSocket.Exception('Connection was closed unexpectably.')
                chunk_table.append(chunk)
                received_so_far += len(chunk)
                timed_out = False
            except TimedSocket.Timeout:
                if timed_out:
                    raise TimedSocket.Exception('Not enough data was received.')
                timed_out = True
        
        # The entire message has been read.
        #print 'recv ->', msg #DEBUG
        return ''.join(chunk_table)
    
    def close(self):
        """
        Wrapper around the close call of native sockets.
        @see: socket.close
        """
        self.sock.close()
        
