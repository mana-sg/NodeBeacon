// src/App.js (Updated Route)
import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import StreamControlPage from './pages/StreamControlPage';
import ViewPage from './pages/ViewPage';
import './App.css';

const PRODUCER_URL = process.env.REACT_APP_PRODUCER_URL || 'http://192.168.2.17:5001';
const VIDEO_SERVER_URL = process.env.REACT_APP_VIDEO_SERVER_URL || 'http://192.168.2.17:5002';
const CHAT_SERVER_URL = process.env.REACT_APP_CHAT_SERVER_URL || 'http://192.168.2.17:5003';

function App() {
  return (
    <BrowserRouter>
      <div className="App">
        <nav className="navigation-bar">
          <Link to="/">Control Stream</Link>
          {/* Example link - update how you generate this */}
          <Link to="/view/live_stream_1">View Stream (Example)</Link>
        </nav>
        <Routes>
          <Route
            path="/"
            element={<StreamControlPage producerBackendUrl={PRODUCER_URL} />}
          />
          {/* --- ADD :streamId PARAMETER --- */}
          <Route
            path="/view/:streamId"
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