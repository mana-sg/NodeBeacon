// src/App.js (Updated)
import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import StreamControlPage from './pages/StreamControlPage';
import ViewPage from './pages/ViewPage';
import './App.css';

// Get URLs from environment variables or use defaults
// Assumes video server runs on 5002 and chat server on 5003
const PRODUCER_URL = process.env.REACT_APP_PRODUCER_URL || 'http://localhost:5001';
const VIDEO_SERVER_URL = process.env.REACT_APP_VIDEO_SERVER_URL || 'http://localhost:5002';
const CHAT_SERVER_URL = process.env.REACT_APP_CHAT_SERVER_URL || 'http://localhost:5003'; // New URL for Chat Server

function App() {
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
            element={<StreamControlPage producerBackendUrl={PRODUCER_URL} />}
          />
          <Route
            path="/view"
            // Pass BOTH URLs to the ViewPage
            element={
              <ViewPage
                videoServerUrl={VIDEO_SERVER_URL}
                chatServerUrl={CHAT_SERVER_URL}
              />
            }
          />
          <Route path="*" element={<div style={{ padding: '20px'}}><h2>404 - Page Not Found</h2><Link to="/">Go Home</Link></div>} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;