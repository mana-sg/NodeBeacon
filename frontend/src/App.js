// src/App.js (Should be correct already)
import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import StreamControlPage from './pages/StreamControlPage'; // Assuming you have this
import ViewPage from './pages/ViewPage';
import './App.css';

// Make sure these URLs are correct for your setup
const PRODUCER_URL = process.env.REACT_APP_PRODUCER_URL || 'http://localhost:5001';
const VIDEO_SERVER_URL = process.env.REACT_APP_VIDEO_SERVER_URL || 'http://localhost:5002'; // This is the consumer server
const CHAT_SERVER_URL = process.env.REACT_APP_CHAT_SERVER_URL || 'http://localhost:5003'; // If you add chat back later

function App() {
  return (
    <BrowserRouter>
      <div className="App">
        <nav className="navigation-bar">
          <Link to="/">Control Stream</Link>
          {/* Example link - update how you generate this if needed */}
          <Link to="/view/live_stream_1">View Stream (Example ID)</Link>
          {/* You might want a dynamic way to list available streams later */}
        </nav>
        <Routes>
          <Route
            path="/"
            element={<StreamControlPage producerBackendUrl={PRODUCER_URL} />} // Pass the producer URL here
          />
          {/* --- Route with :streamId PARAMETER --- */}
          <Route
            path="/view/:streamId"
            element={
              <ViewPage
                videoServerUrl={VIDEO_SERVER_URL} // Pass consumer URL
                chatServerUrl={CHAT_SERVER_URL} // Pass chat URL if needed
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
