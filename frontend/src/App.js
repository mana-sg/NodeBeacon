// frontend/src/App.js
import React, { useState, useEffect, useRef, useCallback } from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import io from 'socket.io-client';
import StreamControlPage from './pages/StreamControlPage';
import ViewPage from './pages/ViewPage';
import './App.css';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://localhost:5001';
const FPS_UPDATE_INTERVAL_MS = 1000; // Update FPS calculation every 1 second

function App() {
  // --- Centralized State ---
  const [currentFrame, setCurrentFrame] = useState(null);
  const [latency, setLatency] = useState(0);
  const [isConnected, setIsConnected] = useState(false);
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamError, setStreamError] = useState(null);
  const [chatMessages, setChatMessages] = useState([]);
  const [calculatedFps, setCalculatedFps] = useState(0); // <-- Add state for FPS
  const socketRef = useRef(null);

  // --- Refs for FPS Calculation ---
  const frameCountRef = useRef(0);
  const lastFpsUpdateTimeRef = useRef(Date.now());

  // --- Socket.IO Connection and Event Listeners ---
  useEffect(() => {
    console.log(`Attempting to connect to backend at ${BACKEND_URL}`);
    socketRef.current = io(BACKEND_URL, {
      reconnectionAttempts: 5,
      transports: ['websocket'],
    });
    const socket = socketRef.current;

    socket.on('connect', () => {
      console.log('🔌 Connected to WebSocket server', socket.id);
      setIsConnected(true);
      setStreamError(null);
      // Reset FPS calculation on new connection
      frameCountRef.current = 0;
      lastFpsUpdateTimeRef.current = Date.now();
      setCalculatedFps(0);
    });

    socket.on('disconnect', (reason) => {
      console.log('🔌 Disconnected from WebSocket server:', reason);
      setIsConnected(false);
      setCurrentFrame(null);
      setLatency(0);
      setIsStreaming(false);
      setStreamError('Disconnected from server');
      setCalculatedFps(0); // <-- Reset FPS on disconnect
    });

    socket.on('connect_error', (error) => {
      console.error('WebSocket connection error:', error);
      setIsConnected(false);
      setStreamError(`Connection Error: ${error.message}`);
      setCalculatedFps(0); // <-- Reset FPS on error
    });

    socket.on('video_frame', (data) => {
      setCurrentFrame(data.image);
      const receiveTime = Date.now(); // Use ms for FPS calc accuracy
      setLatency((receiveTime / 1000 - data.timestamp) * 1000); // Latency calc

      // --- FPS Calculation ---
      frameCountRef.current++;
      const timeNow = Date.now();
      const timeDiff = timeNow - lastFpsUpdateTimeRef.current;

      if (timeDiff >= FPS_UPDATE_INTERVAL_MS) {
        const fps = (frameCountRef.current / (timeDiff / 1000)).toFixed(1); // Calculate FPS
        setCalculatedFps(fps); // Update state to trigger re-render in pages
        frameCountRef.current = 0; // Reset counter
        lastFpsUpdateTimeRef.current = timeNow; // Update timestamp
      }
      // --- End FPS Calculation ---
    });

    socket.on('new_chat_message', (message) => {
      setChatMessages(prevMessages => [...prevMessages, message].slice(-50));
    });

    socket.on('stream_status', (status) => {
      console.log('Received stream status:', status);
      const streamingChanged = isStreaming !== status.active; // Check if status actually changed
      setIsStreaming(status.active);
      setStreamError(status.error);
      // Reset FPS if streaming stops
      if (!status.active && streamingChanged) {
          setCalculatedFps(0);
          frameCountRef.current = 0;
          lastFpsUpdateTimeRef.current = Date.now();
      }
    });

    return () => {
      console.log('Disconnecting WebSocket...');
      socket.disconnect();
      setIsConnected(false);
      setIsStreaming(false);
      setCurrentFrame(null);
      setCalculatedFps(0); // Reset FPS on component unmount
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Added eslint disable comment as isStreaming is used in effect but shouldn't be dependency


  // --- Action Handlers (to be passed down) ---
  const handleStartStream = useCallback(() => {
    if (socketRef.current && isConnected) {
      console.log('Requesting stream start...');
      // Reset FPS counters when explicitly starting
      frameCountRef.current = 0;
      lastFpsUpdateTimeRef.current = Date.now();
      setCalculatedFps(0);
      socketRef.current.emit('start_stream');
    }
  }, [isConnected]);

  const handleStopStream = useCallback(() => {
    if (socketRef.current && isConnected) {
      console.log('Requesting stream stop...');
      socketRef.current.emit('stop_stream');
      // Status update from backend will reset FPS state
    }
  }, [isConnected]);

  const handleSendMessage = useCallback((messageText) => {
    if (messageText.trim() && socketRef.current && isConnected) {
      socketRef.current.emit('send_chat_message', { message: messageText });
    }
  }, [isConnected]);

  // --- Props bundles for pages ---
  const commonProps = {
    isConnected,
    isStreaming,
    streamError,
    currentFrame,
    latency,
    calculatedFps, // <-- Pass calculated FPS
    chatMessages,
    onSendMessage: handleSendMessage,
  };

  const controlProps = {
    ...commonProps,
    onStartStream: handleStartStream,
    onStopStream: handleStopStream,
  };

  return (
    <BrowserRouter>
      <div className="App">
        <nav className="navigation-bar">
          <Link to="/">Control Stream</Link>
          <Link to="/view">View Stream</Link>
        </nav>
        <Routes>
          <Route
            path="/"
            element={<StreamControlPage {...controlProps} />}
          />
          <Route
            path="/view"
            element={<ViewPage {...commonProps} />}
          />
           <Route path="*" element={<div><h2>Page Not Found</h2><Link to="/">Go Home</Link></div>} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;