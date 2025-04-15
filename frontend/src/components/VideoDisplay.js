// frontend/src/components/VideoDisplay.js
import React from 'react';

// Removed latency prop as it's displayed higher up now
function VideoDisplay({ currentFrame, isStreaming }) {
  return (
    <div className="video-container-inner">
      {currentFrame ? (
        <img src={currentFrame} alt="Live video feed" />
      ) : (
        <div className="placeholder">
          {isStreaming ? 'Connecting to stream...' : 'Streaming is inactive.'}
        </div>
      )}
      {/* Removed latency display from here */}
    </div>
  );
}

export default VideoDisplay;