// frontend/src/pages/StreamControlPage.js
import React from 'react';
import VideoDisplay from '../components/VideoDisplay';
import ChatInterface from '../components/ChatInterface';

// Assume props are passed down from App.js containing all necessary state and handlers
function StreamControlPage({
  isConnected,
  isStreaming,
  streamError,
  currentFrame,
  latency,
  chatMessages,
  onStartStream, // Function to start streaming
  onStopStream,  // Function to stop streaming
  onSendMessage  // Function to send chat messages
}) {

  return (
    <div className="page-container">
       <header className="App-header">
         <h1>Kafka Stream Control</h1>
         <div className="status-section">
           <div className={`status-indicator ${isConnected ? 'connected' : 'disconnected'}`}>
             {isConnected ? '● Connected' : '○ Disconnected'}
           </div>
           <div className={`status-indicator ${isStreaming ? 'streaming' : 'stopped'}`}>
             {isStreaming ? '● Streaming Active' : '○ Streaming Inactive'}
           </div>
         </div>
         {streamError && <div className="error-message">Stream Error: {streamError}</div>}
       </header>

      <div className="main-content">
        <div className="video-container">
           <div className="video-header">
              <h2>Live Stream Feed</h2>
              {/* Stream Control Buttons */}
              <div className="stream-controls">
                <button onClick={onStartStream} disabled={!isConnected || isStreaming}>
                  Start Streaming
                </button>
                <button onClick={onStopStream} disabled={!isConnected || !isStreaming}>
                  Stop Streaming
                </button>
              </div>
           </div>
           <VideoDisplay
             currentFrame={currentFrame}
             isStreaming={isStreaming}
             latency={latency}
           />
         </div>

        <ChatInterface
          chatMessages={chatMessages}
          isConnected={isConnected}
          onSendMessage={onSendMessage}
        />
      </div>
    </div>
  );
}

export default StreamControlPage;