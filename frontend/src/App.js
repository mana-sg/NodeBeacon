import React from 'react';
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import StreamControlPage from './pages/StreamControlPage';
import ViewPage from './pages/ViewPage';
import './App.css';

// Get URLs from environment variables
const PRODUCER_URL = process.env.REACT_APP_PRODUCER_URL || 'http://localhost:5001';
const CONSUMER_URL = process.env.REACT_APP_CONSUMER_URL || 'http://localhost:5002';

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
            element={<StreamControlPage producerBackendUrl={PRODUCER_URL} />} // Pass producer URL
          />
          <Route
            path="/view"
            element={<ViewPage consumerBackendUrl={CONSUMER_URL} />} // Pass consumer URL
          />
           <Route path="*" element={<div style={{ padding: '20px'}}><h2>404 - Page Not Found</h2><Link to="/">Go Home</Link></div>} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;