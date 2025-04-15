// frontend/src/pages/ViewPage.js
import React from 'react';
import VideoDisplay from '../components/VideoDisplay';
import ChatInterface from '../components/ChatInterface';

function ViewPage({
  isConnected,
  isStreaming,
  streamError,
  currentFrame,
  latency,
  calculatedFps, // <-- Receive calculated FPS prop
  chatMessages,
  onSendMessage
}) {

  return (
    <div className="page-container">
       <header className="App-header">
         <h1>View Kafka Stream</h1>
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
               {/* Display FPS and Latency Info */}
               <div className="stream-info">
                 {isStreaming && latency > 0 && <p>Latency: {latency.toFixed(1)} ms</p>}
                 {isStreaming && calculatedFps > 0 && <p>Received FPS: {calculatedFps}</p>} {/* <-- Display FPS */}
               </div>
            </div>
            <VideoDisplay
              currentFrame={currentFrame}
              isStreaming={isStreaming}
              latency={latency} // Pass latency down (optional, as it's displayed above now)
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

export default ViewPage;