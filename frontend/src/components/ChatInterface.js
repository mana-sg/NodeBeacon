// frontend/src/components/ChatInterface.js
import React, { useState, useRef, useEffect, useCallback } from 'react';

function ChatInterface({ chatMessages, isConnected, onSendMessage }) {
  const [chatInput, setChatInput] = useState('');
  const chatEndRef = useRef(null); // Ref to scroll chat to bottom

  // Scroll chat to the bottom when new messages arrive
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [chatMessages]);

  const handleInputChange = (e) => {
    setChatInput(e.target.value);
  };

  const handleSubmit = useCallback((e) => {
    e.preventDefault();
    if (chatInput.trim() && isConnected) {
      onSendMessage(chatInput); // Call the passed-in function
      setChatInput(''); // Clear the input field
    }
  }, [chatInput, isConnected, onSendMessage]);

  return (
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
      <form className="chat-input-form" onSubmit={handleSubmit}>
        <input
          type="text"
          value={chatInput}
          onChange={handleInputChange}
          placeholder="Type your message..."
          disabled={!isConnected}
        />
        <button type="submit" disabled={!isConnected || !chatInput.trim()}>Send</button>
      </form>
    </div>
  );
}

export default ChatInterface;