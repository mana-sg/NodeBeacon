// src/pages/ViewPage.js (Corrected Cleanup)
import React, { useState, useEffect, useRef, useCallback } from 'react';
import io from 'socket.io-client';
import ChatInterface from '../components/ChatInterface';
import './ViewPage.css'; // Create or ensure this CSS file exists and is styled

const FPS_UPDATE_INTERVAL_MS = 1000;
const STALE_FRAME_THRESHOLD_MS = 5000; // 5 seconds for staleness check
const STALE_CHECK_INTERVAL_MS = 2000; // Check every 2 seconds

function ViewPage({ videoServerUrl, chatServerUrl }) { // Receive both URLs
  // --- State ---
  const [videoSocketConnected, setVideoSocketConnected] = useState(false);
  const [chatSocketConnected, setChatSocketConnected] = useState(false);
  const [currentFrame, setCurrentFrame] = useState(null);
  const [latency, setLatency] = useState(0);
  const [chatMessages, setChatMessages] = useState([]);
  const [calculatedFps, setCalculatedFps] = useState(0);
  const [videoError, setVideoError] = useState(null); // Error specific to video
  const [chatError, setChatError] = useState(null); // Error specific to chat

  // --- Refs ---
  const videoSocketRef = useRef(null);
  const chatSocketRef = useRef(null);
  const frameCountRef = useRef(0);
  const lastFpsUpdateTimeRef = useRef(Date.now());
  const lastFrameTimestampRef = useRef(0);

  // --- Video Socket Connection ---
  useEffect(() => {
    if (!videoServerUrl) {
        console.warn("ViewPage: videoServerUrl is not provided.");
        return; // Don't attempt connection if URL is missing
    }
    if (videoSocketRef.current) {
        // Cleanup existing connection if URL changes or component re-renders unexpectedly
        console.log("ViewPage: Cleaning up previous video socket before reconnecting.");
        videoSocketRef.current.disconnect();
        videoSocketRef.current = null;
    }


    console.log(`ViewPage: Attempting to connect to Video Server at ${videoServerUrl}`);
    videoSocketRef.current = io(videoServerUrl, {
      reconnectionAttempts: 5,
      transports: ['websocket'],
      timeout: 10000,
    });
    const socket = videoSocketRef.current;

    socket.on('connect', () => {
      console.log('ViewPage: Connected to Video Server', socket.id);
      setVideoSocketConnected(true);
      setVideoError(null);
      // Reset video-specific state
      frameCountRef.current = 0;
      lastFpsUpdateTimeRef.current = Date.now();
      lastFrameTimestampRef.current = 0;
      setCalculatedFps(0);
      setLatency(0);
      setCurrentFrame(null);
    });

    socket.on('disconnect', (reason) => {
      console.log('ViewPage: Disconnected from Video Server:', reason);
      setVideoSocketConnected(false);
      setCurrentFrame(null);
      setLatency(0);
      setCalculatedFps(0);
      setVideoError(`Video Disconnected: ${reason}`);
    });

    socket.on('connect_error', (error) => {
      console.error('ViewPage: Video connection error:', error);
      setVideoSocketConnected(false);
      setVideoError(`Video Connection Error: ${error.message || 'Unknown error'}`);
    });

    socket.on('video_frame', (data) => {
      if (!data || !data.image) {
        // console.warn("Received invalid video frame data:", data); // Can be verbose
        return;
      }
      setCurrentFrame(data.image);
      const receiveTime = Date.now();
      lastFrameTimestampRef.current = receiveTime;

      if (typeof data.latency === 'number') {
          setLatency(data.latency);
      } else if (typeof data.timestamp === 'number') { // Fallback calculation
           setLatency((receiveTime / 1000 - data.timestamp) * 1000);
      }

      // FPS Calculation
      frameCountRef.current++;
      const timeNow = Date.now();
      const timeDiff = timeNow - lastFpsUpdateTimeRef.current;
      if (timeDiff >= FPS_UPDATE_INTERVAL_MS) {
        const fps = (frameCountRef.current / (timeDiff / 1000)).toFixed(1);
        setCalculatedFps(fps);
        frameCountRef.current = 0;
        lastFpsUpdateTimeRef.current = timeNow;
      }

      if (videoError) setVideoError(null); // Clear video error if frames arrive
    });

    // Cleanup function for this effect
    return () => {
      console.log('ViewPage: Cleaning up video socket...');
      if (videoSocketRef.current) {
          videoSocketRef.current.disconnect();
          videoSocketRef.current = null; // Clear the ref
      }
      // Reset state associated with this socket
      setVideoSocketConnected(false);
      setCurrentFrame(null);
      setCalculatedFps(0);
      setLatency(0);
      setVideoError(null);
    };
  }, [videoServerUrl]); // Re-run ONLY if videoServerUrl changes


  // --- Chat Socket Connection ---
  useEffect(() => {
    if (!chatServerUrl) {
        console.warn("ViewPage: chatServerUrl is not provided.");
        return; // Don't attempt connection if URL is missing
    }
     if (chatSocketRef.current) {
        // Cleanup existing connection if URL changes or component re-renders unexpectedly
        console.log("ViewPage: Cleaning up previous chat socket before reconnecting.");
        chatSocketRef.current.disconnect();
        chatSocketRef.current = null;
    }

    console.log(`ViewPage: Attempting to connect to Chat Server at ${chatServerUrl}`);
    chatSocketRef.current = io(chatServerUrl, {
      reconnectionAttempts: 5,
      transports: ['websocket'],
      timeout: 10000,
    });
    const socket = chatSocketRef.current;

    socket.on('connect', () => {
      console.log('ViewPage: Connected to Chat Server', socket.id);
      setChatSocketConnected(true);
      setChatError(null);
    });

    socket.on('disconnect', (reason) => {
      console.log('ViewPage: Disconnected from Chat Server:', reason);
      setChatSocketConnected(false);
      setChatError(`Chat Disconnected: ${reason}`);
    });

    socket.on('connect_error', (error) => {
      console.error('ViewPage: Chat connection error:', error);
      setChatSocketConnected(false);
      setChatError(`Chat Connection Error: ${error.message || 'Unknown error'}`);
    });

    socket.on('new_chat_message', (message) => {
      // Added more robust check
      if (!message || typeof message !== 'object' || !message.id || typeof message.sender === 'undefined' || typeof message.text === 'undefined' || typeof message.timestamp === 'undefined') {
        console.warn("Received invalid chat message structure:", message);
        return;
      }
      setChatMessages(prevMessages => {
          // Prevent duplicates by ID
          if (prevMessages.some(m => m.id === message.id)) {
              return prevMessages;
          }
          const updatedMessages = [...prevMessages, message];
          return updatedMessages.slice(-100); // Keep last 100
      });
      if (chatError) setChatError(null); // Clear chat error if messages arrive
    });

    socket.on('chat_error', (errorData) => {
       console.error("ViewPage: Chat Error from server:", errorData.error);
       setChatError(errorData.error || 'An unknown chat error occurred.');
    });

    // Cleanup function for this effect
    return () => {
      console.log('ViewPage: Cleaning up chat socket...');
      if (chatSocketRef.current) {
          chatSocketRef.current.disconnect();
          chatSocketRef.current = null; // Clear the ref
      }
       // Reset state associated with this socket
      setChatSocketConnected(false);
      // Consider whether to clear messages on disconnect - personal preference
      // setChatMessages([]);
      setChatError(null);
    };
  }, [chatServerUrl]); // Re-run ONLY if chatServerUrl changes


  // Effect for stale video frames check
  useEffect(() => {
    let staleTimer = null;
    if (videoSocketConnected && currentFrame) {
      // Only start the timer if we are connected AND have received at least one frame
      lastFrameTimestampRef.current = Date.now(); // Reset timestamp when check starts
      staleTimer = setInterval(() => {
        const timeSinceLastFrame = Date.now() - lastFrameTimestampRef.current;
        if (timeSinceLastFrame > STALE_FRAME_THRESHOLD_MS) {
          if (!videoError?.includes("paused")) { // Avoid setting error repeatedly
             setVideoError("Stream seems paused (no frames received recently).");
          }
          setCalculatedFps(0); // Reset FPS if stream is stale
          // We don't clear the frame here, just show the error
        }
      }, STALE_CHECK_INTERVAL_MS);
    } else {
        // If not connected or no frame, ensure timer is clear
        if (staleTimer) clearInterval(staleTimer);
    }
    // Cleanup timer on unmount or when dependencies change
    return () => {
        if (staleTimer) clearInterval(staleTimer);
    };
  }, [videoSocketConnected, currentFrame, videoError]); // Rerun if connection/frame/error state changes


  // --- Handler for Sending Chat Messages ---
  const handleSendMessage = useCallback((messageText) => {
    // Use the CHAT socket ref and CHAT connection state
    if (chatSocketRef.current && chatSocketConnected && messageText.trim()) {
      console.log("ViewPage: Sending chat message via Chat Socket:", messageText);
      chatSocketRef.current.emit('send_chat_message', { message: messageText });
      // Optimistically clear local chat error, server might still send one back
      setChatError(null);
    } else if (!chatSocketConnected) {
      console.warn('ViewPage: Cannot send chat message, chat socket not connected.');
      setChatError('Cannot send message: Chat not connected.'); // Show error locally
    } else {
      // Don't show error for empty message, just ignore
      console.warn('ViewPage: Cannot send empty chat message.');
    }
  }, [chatSocketConnected]); // Depends only on chat connection status


  // --- Render Logic ---
  return (
    <div className="view-page">
      <h1>Live Stream Viewer</h1>

      <div className="status-bar">
          {/* Video Status */}
          <span>Video: <span className={videoSocketConnected ? 'status-ok' : 'status-error'}>{videoSocketConnected ? 'Connected' : 'Disconnected'}</span></span>
          {videoSocketConnected && currentFrame && !videoError && <span> | Status: <span className='status-ok'>Receiving</span></span>}
          {videoSocketConnected && !currentFrame && !videoError && <span> | Status: <span className='status-warn'>Waiting...</span></span>}
          {videoSocketConnected && currentFrame && !videoError && <span> | Latency: {latency.toFixed(1)} ms</span>}
          {videoSocketConnected && currentFrame && !videoError && <span> | Recv FPS: {calculatedFps}</span>}

          {/* Separator */}
          <span style={{ margin: '0 10px' }}>||</span>

           {/* Chat Status */}
           <span>Chat: <span className={chatSocketConnected ? 'status-ok' : 'status-error'}>{chatSocketConnected ? 'Connected' : 'Disconnected'}</span></span>

           {/* Video Errors (display beside video status) */}
           {videoError && <span className="error-message"> | Video Status: {videoError}</span>}
           {/* Chat errors are handled within ChatInterface */}
       </div>

       <div className="content-area">
            {/* Video Area */}
            <div className="video-container">
                 <h2>Video Feed</h2>
                 <div className="video-display">
                    {currentFrame && videoSocketConnected ? (
                        <img src={currentFrame} alt="Live Stream" />
                    ) : (
                        <div className="video-placeholder">
                           {/* More informative placeholder */}
                           {videoError ? videoError : (videoSocketConnected ? "Waiting for stream..." : "Video Disconnected")}
                        </div>
                    )}
                </div>
            </div>

           {/* Chat Area - Pass chat-specific state */}
           <ChatInterface
                messages={chatMessages}
                isConnected={chatSocketConnected} // Pass chat connection status
                onSendMessage={handleSendMessage}
                chatError={chatError} // Pass down the chat error state
           />
       </div>
    </div>
  );
}

export default ViewPage;