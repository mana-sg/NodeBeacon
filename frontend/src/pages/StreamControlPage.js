import React, { useState, useEffect, useRef, useCallback } from 'react';
import io from 'socket.io-client';
import './StreamControlPage.css'; // Create this CSS file if needed

function StreamControlPage({ producerBackendUrl }) {
  const [isConnected, setIsConnected] = useState(false);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamError, setStreamError] = useState(null);
  const socketRef = useRef(null);

  // --- Socket.IO Connection and Event Listeners ---
  useEffect(() => {
    console.log(`ControlPage: Attempting to connect to producer at ${producerBackendUrl}`);
    // Ensure we disconnect any previous socket if URL changes (though unlikely here)
    if (socketRef.current) {
        socketRef.current.disconnect();
    }

    socketRef.current = io(producerBackendUrl, {
      reconnectionAttempts: 5,
      transports: ['websocket'],
    });
    const socket = socketRef.current;

    socket.on('connect', () => {
      console.log('ControlPage: Connected to producer server', socket.id);
      setIsConnected(true);
      setStreamError(null); // Clear error on successful connect
       // Request initial status on connect just in case we missed the first emit
       // (The server already sends it, but this is a fallback)
       // socket.emit('get_status'); // Optional: Need corresponding handler on server
    });

    socket.on('disconnect', (reason) => {
      console.log('ControlPage: Disconnected from producer server:', reason);
      setIsConnected(false);
      setIsStreaming(false); // Assume streaming stopped if disconnected
      setStreamError('Disconnected from producer control server');
    });

    socket.on('connect_error', (error) => {
      console.error('ControlPage: Producer connection error:', error);
      setIsConnected(false);
      setIsStreaming(false);
      setStreamError(`Producer Connection Error: ${error.message}`);
    });

    // Listen for status updates FROM the producer server
    socket.on('stream_status', (status) => {
      console.log('ControlPage: Received stream status:', status);
      setIsStreaming(status.active);
      setStreamError(status.error);
    });

    // Cleanup on component unmount
    return () => {
      console.log('ControlPage: Disconnecting from producer server...');
      if (socketRef.current) {
          socketRef.current.disconnect();
      }
      setIsConnected(false);
      setIsStreaming(false);
    };
  }, [producerBackendUrl]); // Re-run effect if backend URL changes

  // --- Action Handlers ---
  const handleStartStream = useCallback(() => {
    if (socketRef.current && isConnected) {
      console.log('ControlPage: Requesting stream start...');
      setStreamError(null); // Clear previous errors on start attempt
      socketRef.current.emit('start_stream');
    } else {
        console.warn('ControlPage: Cannot start stream, not connected.');
        setStreamError('Not connected to producer server.');
    }
  }, [isConnected]);

  const handleStopStream = useCallback(() => {
    if (socketRef.current && isConnected) {
      console.log('ControlPage: Requesting stream stop...');
      socketRef.current.emit('stop_stream');
    } else {
        console.warn('ControlPage: Cannot stop stream, not connected.');
    }
  }, [isConnected]);


  return (
    <div className="stream-control-page">
      <h2>Stream Control</h2>
      <div className="status-section">
        <p>Control Connection: <span className={isConnected ? 'status-ok' : 'status-error'}>{isConnected ? 'Connected' : 'Disconnected'}</span></p>
        <p>Producer Status: <span className={isStreaming ? 'status-ok' : 'status-warn'}>{isStreaming ? 'Streaming Active' : 'Inactive'}</span></p>
        {streamError && <p className="error-message">Error: {streamError}</p>}
      </div>
      <div className="controls-section">
        <button onClick={handleStartStream} disabled={!isConnected || isStreaming}>
          Start Streaming
        </button>
        <button onClick={handleStopStream} disabled={!isConnected || !isStreaming}>
          Stop Streaming
        </button>
      </div>
       {/* Simple instructions or info */}
       <div className="info-section">
          <p>Use these buttons to start or stop the video producer.</p>
          <p>The producer captures video and sends it to the central Kafka queue.</p>
          <p>Go to the 'View Stream' page to see the video output.</p>
          <p>Producer URL: {producerBackendUrl}</p>
       </div>
    </div>
  );
}

export default StreamControlPage;