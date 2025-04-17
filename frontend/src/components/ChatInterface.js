// src/components/ChatInterface.js (Updated)
import React, { useState, useRef, useEffect, useCallback } from 'react';
import './ChatInterface.css'; // Make sure you have styles for this

// Renamed chatMessages -> messages to match common usage
function ChatInterface({ messages, isConnected, onSendMessage, chatError }) {
  const [chatInput, setChatInput] = useState('');
  const chatEndRef = useRef(null);

  useEffect(() => {
    // Scroll chat to the bottom when new messages arrive
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]); // Depends only on messages changing

  const handleInputChange = (e) => {
    setChatInput(e.target.value);
  };

  // Use the callback passed from the parent to send the message
  const handleSubmit = useCallback((e) => {
    e.preventDefault();
    const trimmedInput = chatInput.trim();
    if (trimmedInput && isConnected) {
      onSendMessage(trimmedInput); // Call the handler from ViewPage
      setChatInput(''); // Clear input after sending
    }
  }, [chatInput, isConnected, onSendMessage]);

  return (
    <div className="chat-container">
      <h2>Live Chat</h2>
      <div className="chat-messages">
        {/* Render messages passed via props */}
        {messages.map((msg) => (
          // Ensure msg has a unique key - assuming msg.id is provided by server
          <div key={msg.id} className="chat-message">
            <span className="sender">{msg.sender || 'User'}:</span> {/* Provide default */}
            <span className="text">{msg.text}</span>
            {/* Optionally display timestamp from message */}
            {msg.timestamp && (
                <span className="timestamp">
                    {new Date(msg.timestamp * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit'})}
                </span>
             )}
          </div>
        ))}
        {/* Element to trigger scroll */}
        <div ref={chatEndRef} />
      </div>
      {/* Display chat-specific errors passed down */}
      {chatError && <div className="chat-error-display">{chatError}</div>}
      <form className="chat-input-form" onSubmit={handleSubmit}>
        <input
          type="text"
          value={chatInput}
          onChange={handleInputChange}
          placeholder={isConnected ? "Type your message..." : "Chat disconnected"}
          disabled={!isConnected} // Disable based on chat connection status prop
          aria-label="Chat message input"
        />
        <button
          type="submit"
          // Disable if not connected OR if input is only whitespace
          disabled={!isConnected || !chatInput.trim()}
        >
          Send
        </button>
      </form>
    </div>
  );
}

export default ChatInterface;