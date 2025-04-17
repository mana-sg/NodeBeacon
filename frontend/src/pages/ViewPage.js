import React, { useState, useEffect, useRef, useCallback } from 'react';
import io from 'socket.io-client';
import './ViewPage.css'; // Create this CSS file

const FPS_UPDATE_INTERVAL_MS = 1000; // Update FPS calculation every 1 second

// Simple sub-components (can be moved to separate files)
function VideoDisplay({ currentFrame }) {
    return (
        <div className="video-display">
            {currentFrame ? (
                <img src={currentFrame} alt="Live Stream" />
            ) : (
                <div className="video-placeholder">Waiting for stream...</div>
            )}
        </div>
    );
}

function ChatWindow({ messages, onSendMessage }) {
    const [inputText, setInputText] = useState('');
    const messagesEndRef = useRef(null);

    const handleSend = () => {
        if (inputText.trim()) {
            onSendMessage(inputText);
            setInputText('');
        }
    };

    const handleKeyPress = (event) => {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault(); // Prevent newline in input
            handleSend();
        }
    };

    useEffect(() => {
        // Scroll to bottom when new messages arrive
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages]);


    return (
        <div className="chat-window">
            <h4>Chat</h4>
            <div className="chat-messages">
                {messages.length === 0 && <p className="no-messages">No messages yet.</p>}
                {messages.map((msg) => (
                    <div key={msg.id} className="chat-message">
                        <span className="sender">{msg.sender}:</span>
                        <span className="text">{msg.text}</span>
                        <span className="timestamp">({new Date(msg.timestamp * 1000).toLocaleTimeString()})</span>
                    </div>
                ))}
                <div ref={messagesEndRef} /> {/* Anchor for scrolling */}
            </div>
            <div className="chat-input">
                <input
                    type="text"
                    value={inputText}
                    onChange={(e) => setInputText(e.target.value)}
                    onKeyPress={handleKeyPress}
                    placeholder="Type your message..."
                />
                <button onClick={handleSend}>Send</button>
            </div>
        </div>
    );
}


function ViewPage({ consumerBackendUrl }) {
  // --- State for this page ---
  const [isConnected, setIsConnected] = useState(false);
  const [currentFrame, setCurrentFrame] = useState(null);
  const [latency, setLatency] = useState(0);
  const [chatMessages, setChatMessages] = useState([]);
  const [calculatedFps, setCalculatedFps] = useState(0);
  const [consumerError, setConsumerError] = useState(null);
  const socketRef = useRef(null);

  // --- Refs for FPS Calculation ---
  const frameCountRef = useRef(0);
  const lastFpsUpdateTimeRef = useRef(Date.now());

  // --- Socket.IO Connection and Event Listeners ---
  useEffect(() => {
    console.log(`ViewPage: Attempting to connect to consumer at ${consumerBackendUrl}`);
    if (socketRef.current) {
        socketRef.current.disconnect();
    }

    socketRef.current = io(consumerBackendUrl, {
      reconnectionAttempts: 5,
      transports: ['websocket'],
      // Add buffer size if needed, though server setting is more critical
      // transportsOptions: {
      //   websocket: {
      //     maxPayload: 10 * 1024 * 1024 // Match server buffer (example)
      //   }
      // }
    });
    const socket = socketRef.current;

    socket.on('connect', () => {
      console.log('ViewPage: Connected to consumer server', socket.id);
      setIsConnected(true);
      setConsumerError(null);
      // Reset FPS calculation on new connection
      frameCountRef.current = 0;
      lastFpsUpdateTimeRef.current = Date.now();
      setCalculatedFps(0);
    });

    socket.on('disconnect', (reason) => {
      console.log('ViewPage: Disconnected from consumer server:', reason);
      setIsConnected(false);
      setCurrentFrame(null); // Clear frame on disconnect
      setLatency(0);
      setCalculatedFps(0); // Reset FPS
      setConsumerError('Disconnected from view server');
    });

    socket.on('connect_error', (error) => {
      console.error('ViewPage: Consumer connection error:', error);
      setIsConnected(false);
      setConsumerError(`View Connection Error: ${error.message}`);
      setCalculatedFps(0); // Reset FPS
      setCurrentFrame(null);
    });

    socket.on('video_frame', (data) => {
      setCurrentFrame(data.image);
      const receiveTime = Date.now();
      setLatency((receiveTime / 1000 - data.timestamp) * 1000); // Calculate latency in ms

      // --- FPS Calculation ---
      frameCountRef.current++;
      const timeNow = Date.now();
      const timeDiff = timeNow - lastFpsUpdateTimeRef.current;

      if (timeDiff >= FPS_UPDATE_INTERVAL_MS) {
        const fps = (frameCountRef.current / (timeDiff / 1000)).toFixed(1);
        setCalculatedFps(fps);
        frameCountRef.current = 0;
        lastFpsUpdateTimeRef.current = timeNow;
      }
      // --- End FPS Calculation ---

      // Clear error if we start receiving frames
      if (consumerError) setConsumerError(null);
    });

    socket.on('new_chat_message', (message) => {
      setChatMessages(prevMessages => {
          // Prevent duplicates just in case
          if (prevMessages.some(m => m.id === message.id)) {
              return prevMessages;
          }
          // Keep max 50 messages
          return [...prevMessages, message].slice(-50);
      });
    });

    // --- NO 'stream_status' listener here ---
    // This page infers streaming activity by receiving frames.
    // It doesn't directly know the producer's intended state.

    // Cleanup
    return () => {
      console.log('ViewPage: Disconnecting from consumer server...');
      if (socketRef.current) {
          socketRef.current.disconnect();
      }
      setIsConnected(false);
      setCurrentFrame(null);
      setCalculatedFps(0);
    };
  }, [consumerBackendUrl]); // Dependency on the backend URL

  // --- Action Handler for Chat ---
  const handleSendMessage = useCallback((messageText) => {
    if (messageText.trim() && socketRef.current && isConnected) {
      socketRef.current.emit('send_chat_message', { message: messageText });
    } else if (!isConnected) {
        console.warn('ViewPage: Cannot send chat message, not connected.');
        // Optionally show an error to the user
    }
  }, [isConnected]);


  return (
    <div className="view-page">
      <h2>Live Stream View</h2>
       <div className="status-section">
          <p>View Connection: <span className={isConnected ? 'status-ok' : 'status-error'}>{isConnected ? 'Connected' : 'Disconnected'}</span></p>
          {/* We don't get direct 'isStreaming' status here, infer from frames */}
          {isConnected && currentFrame && <p>Status: <span className='status-ok'>Receiving Frames</span></p>}
          {isConnected && !currentFrame && <p>Status: <span className='status-warn'>Connected, waiting for frames...</span></p>}
          {consumerError && <p className="error-message">Error: {consumerError}</p>}
          {isConnected && currentFrame && <p>Approx Latency: {latency.toFixed(1)} ms</p>}
          {isConnected && currentFrame && <p>Received FPS: {calculatedFps}</p>}
       </div>
       <div className="main-content">
           <VideoDisplay currentFrame={currentFrame} />
           <ChatWindow messages={chatMessages} onSendMessage={handleSendMessage} />
       </div>
        <div className="info-section">
             <p>Consumer URL: {consumerBackendUrl}</p>
        </div>
    </div>
  );
}

export default ViewPage;