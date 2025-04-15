// frontend/src/App.js
import React, { useState, useEffect, useRef, useCallback } from 'react';
import io from 'socket.io-client';
import './App.css'; // Basic styling

// Define the backend URL (adjust if your backend runs elsewhere)
const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://localhost:5001';

function App() {
  const [currentFrame, setCurrentFrame] = useState(null);
  const [latency, setLatency] = useState(0);
  const [isConnected, setIsConnected] = useState(false);
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState('');
  const socketRef = useRef(null);
  const chatEndRef = useRef(null); // Ref to scroll chat to bottom

  // Scroll chat to the bottom when new messages arrive
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [chatMessages]);

  // Effect for Socket.IO connection and event listeners
  useEffect(() => {
    // Connect to the Socket.IO server
    console.log(`Attempting to connect to backend at ${BACKEND_URL}`);
    socketRef.current = io(BACKEND_URL, {
      reconnectionAttempts: 5, // Limit reconnection attempts
      transports: ['websocket'], // Prefer WebSocket
    });

    const socket = socketRef.current;

    // --- Connection Events ---
    socket.on('connect', () => {
      console.log('🔌 Connected to WebSocket server', socket.id);
      setIsConnected(true);
    });

    socket.on('disconnect', (reason) => {
      console.log('🔌 Disconnected from WebSocket server:', reason);
      setIsConnected(false);
      setCurrentFrame(null); // Clear frame on disconnect
      setLatency(0);
    });

    socket.on('connect_error', (error) => {
      console.error('WebSocket connection error:', error);
      setIsConnected(false);
    });

    // --- Custom Event Listeners ---
    socket.on('video_frame', (data) => {
      setCurrentFrame(data.image); // data.image contains the 'data:image/jpeg;base64,...' string
      // Calculate latency using the timestamp sent from the backend
      const currentTimestamp = Date.now() / 1000; // seconds
      setLatency((currentTimestamp - data.timestamp) * 1000); // ms
    });

    socket.on('new_chat_message', (message) => {
      // Add new message, keeping only the last N messages (e.g., 50)
      setChatMessages(prevMessages => [...prevMessages, message].slice(-50));
    });

    // --- Cleanup Function ---
    // This runs when the component unmounts
    return () => {
      console.log('Disconnecting WebSocket...');
      socket.disconnect();
      setIsConnected(false);
      setCurrentFrame(null);
    };
  }, []); // Empty dependency array ensures this runs only once on mount

  // --- Chat Sending ---
  const handleSendMessage = useCallback((e) => {
    e.preventDefault(); // Prevent form submission page reload
    if (chatInput.trim() && socketRef.current && isConnected) {
      socketRef.current.emit('send_chat_message', { message: chatInput });
      setChatInput(''); // Clear the input field
    }
  }, [chatInput, isConnected]); // Dependencies for the callback

  return (
    <div className="App">
      <header className="App-header">
        <h1>Kafka Video Stream & Chat</h1>
        <div className={`status-indicator ${isConnected ? 'connected' : 'disconnected'}`}>
          {isConnected ? '● Connected' : '○ Disconnected'}
        </div>
      </header>

      <div className="main-content">
        <div className="video-container">
          <h2>Live Stream</h2>
          {currentFrame ? (
            <img src={currentFrame} alt="Live video feed" />
          ) : (
            <div className="placeholder">Waiting for video stream...</div>
          )}
          {latency > 0 && <p>Latency: {latency.toFixed(1)} ms</p>}
        </div>

        <div className="chat-container">
          <h2>Live Chat</h2>
          <div className="chat-messages">
            {chatMessages.map((msg) => (
              <div key={msg.id} className="chat-message">
                <span className="sender">{msg.sender || 'Anon'}:</span>
                <span className="text">{msg.text}</span>
              </div>
            ))}
            {/* Empty div to scroll to */}
            <div ref={chatEndRef} />
          </div>
          <form className="chat-input-form" onSubmit={handleSendMessage}>
            <input
              type="text"
              value={chatInput}
              onChange={(e) => setChatInput(e.target.value)}
              placeholder="Type your message..."
              disabled={!isConnected}
            />
            <button type="submit" disabled={!isConnected || !chatInput.trim()}>Send</button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default App;