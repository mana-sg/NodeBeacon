// src/pages/ViewPage.js (MODIFIED - Join Room & Viewer Count)
import React, { useState, useEffect, useRef, useCallback } from 'react';
import io from 'socket.io-client';
import { useParams } from 'react-router-dom';
import ChatInterface from '../components/ChatInterface';
import './ViewPage.css';

const FPS_UPDATE_INTERVAL_MS = 1000;
const STALE_FRAME_THRESHOLD_MS = 5000;
const STALE_CHECK_INTERVAL_MS = 2000;
const HISTORY_LIMIT = 20

function ViewPage({ videoServerUrl, chatServerUrl }) {
    const { streamId } = useParams();

    // --- State ---
    const [videoSocketConnected, setVideoSocketConnected] = useState(false);
    const [chatSocketConnected, setChatSocketConnected] = useState(false);
    const [currentFrame, setCurrentFrame] = useState(null);
    const [latency, setLatency] = useState(0);
    const [chatMessages, setChatMessages] = useState([]);
    const [calculatedFps, setCalculatedFps] = useState(0);
    const [videoError, setVideoError] = useState(null);
    const [chatError, setChatError] = useState(null);
    const [isLoadingHistory, setIsLoadingHistory] = useState(false);
    const [viewerCount, setViewerCount] = useState(0); // <<< New state for viewer count

    // --- Refs ---
    const videoSocketRef = useRef(null);
    const chatSocketRef = useRef(null);
    const frameCountRef = useRef(0);
    const lastFpsUpdateTimeRef = useRef(Date.now());
    const lastFrameTimestampRef = useRef(0);
    const historyLoadedRef = useRef(false);

    // --- Fetch Chat History Function (Unchanged) ---
    const fetchChatHistory = useCallback(async () => {
        // ... (fetchChatHistory code remains the same) ...
        if (!streamId || historyLoadedRef.current) return;
        console.log(`ViewPage: Fetching chat history for stream: ${streamId}`);
        setIsLoadingHistory(true); setChatError(null); historyLoadedRef.current = true;
        try {
            const historyUrl = `${chatServerUrl.replace(/\/$/, '')}/chat/history/${streamId}`;
            const response = await fetch(historyUrl);
            if (!response.ok) { const errorData = await response.json(); throw new Error(errorData.error || `HTTP error! status: ${response.status}`); }
            const history = await response.json();
            const formattedHistory = history.map(msg => ({ ...msg, id: msg.id || `${msg.timestamp}-${msg.sender}` }));
            setChatMessages(formattedHistory);
        } catch (error) { console.error("ViewPage: Failed to fetch chat history:", error); setChatError(`Failed to load chat history: ${error.message}`); historyLoadedRef.current = false;
        } finally { setIsLoadingHistory(false); }
    }, [streamId, chatServerUrl]);


    // --- Video Socket Connection (Add join_stream emit and viewer_count listener) ---
    useEffect(() => {
        if (!videoServerUrl) return;
        if (videoSocketRef.current) { videoSocketRef.current.disconnect(); videoSocketRef.current = null; }

        console.log(`ViewPage: Attempting to connect to Video Server at ${videoServerUrl}`);
        videoSocketRef.current = io(videoServerUrl, { /* ... options ... */ });
        const socket = videoSocketRef.current;

        socket.on('connect', () => {
            console.log('ViewPage: Connected to Video Server', socket.id);
            setVideoSocketConnected(true); setVideoError(null);
            // --- EMIT join_stream ---
            if (streamId) {
                console.log(`ViewPage: Emitting join_stream for ${streamId} to Video Server`);
                socket.emit('join_stream', { stream_id: streamId });
            } else {
                console.warn("ViewPage: No streamId found in URL, cannot join video room.");
                setVideoError("Missing stream ID in URL.");
            }
            // Reset state...
             frameCountRef.current = 0; lastFpsUpdateTimeRef.current = Date.now(); lastFrameTimestampRef.current = 0; setCalculatedFps(0); setLatency(0); setCurrentFrame(null); setViewerCount(0); // Reset viewer count too
        });

        socket.on('disconnect', (reason) => { setVideoSocketConnected(false); setVideoError(`Video Disconnected: ${reason}`); setCurrentFrame(null); setLatency(0); setCalculatedFps(0); setViewerCount(0); });
        socket.on('connect_error', (error) => { setVideoSocketConnected(false); setVideoError(`Video Connection Error: ${error.message || 'Unknown error'}`); });
        socket.on('video_frame', (data) => { /* ... process frame, latency, FPS ... (unchanged) */
            if (!data || !data.image) return;
            setCurrentFrame(data.image);
            const receiveTime = Date.now(); lastFrameTimestampRef.current = receiveTime;
            if (typeof data.latency === 'number') setLatency(data.latency);
            else if (typeof data.timestamp === 'number') setLatency((receiveTime / 1000 - data.timestamp) * 1000);
            frameCountRef.current++; const timeNow = Date.now(); const timeDiff = timeNow - lastFpsUpdateTimeRef.current;
            if (timeDiff >= FPS_UPDATE_INTERVAL_MS) { setCalculatedFps((frameCountRef.current / (timeDiff / 1000)).toFixed(1)); frameCountRef.current = 0; lastFpsUpdateTimeRef.current = timeNow; }
            if (videoError) setVideoError(null);
         });

        // --- LISTEN for viewer_count_update ---
        socket.on('viewer_count_update', (data) => {
            // Ensure the update is for the stream we are currently viewing
            if (data && data.stream_id === streamId) {
                // console.log("Viewer count update:", data.count); // Can be verbose
                setViewerCount(data.count);
            }
        });

        // Cleanup
        return () => {
            console.log('ViewPage: Cleaning up video socket...');
            if (videoSocketRef.current) {
                // Optionally emit leave_stream if needed by backend logic
                // if (streamId) { videoSocketRef.current.emit('leave_stream', { stream_id: streamId }); }
                videoSocketRef.current.disconnect();
                videoSocketRef.current = null;
            }
            setVideoSocketConnected(false); setCurrentFrame(null); setCalculatedFps(0); setLatency(0); setVideoError(null); setViewerCount(0); // Reset viewer count on cleanup
        };
    }, [videoServerUrl, streamId]); // Add streamId to dependency array


    // --- Chat Socket Connection (Unchanged) ---
    useEffect(() => {
        // ... (Chat socket connection logic remains the same as previous version) ...
        if (!chatServerUrl) return;
        if (chatSocketRef.current) { chatSocketRef.current.disconnect(); chatSocketRef.current = null; }
        console.log(`ViewPage: Attempting to connect to Chat Server at ${chatServerUrl}`);
        historyLoadedRef.current = false;
        chatSocketRef.current = io(chatServerUrl, { /* ... options ... */ });
        const socket = chatSocketRef.current;
        socket.on('connect', () => { console.log('ViewPage: Connected to Chat Server', socket.id); setChatSocketConnected(true); setChatError(null); fetchChatHistory(); });
        socket.on('disconnect', (reason) => { console.log('ViewPage: Disconnected from Chat Server:', reason); setChatSocketConnected(false); setChatError(`Chat Disconnected: ${reason}`); historyLoadedRef.current = false; });
        socket.on('connect_error', (error) => { console.error('ViewPage: Chat connection error:', error); setChatSocketConnected(false); setChatError(`Chat Connection Error: ${error.message || 'Unknown error'}`); historyLoadedRef.current = false; });
        socket.on('new_chat_message', (message) => {
            if (!message || typeof message !== 'object' || !message.id || !message.stream_id ) return;
            if (message.stream_id === streamId) {
                 // console.log("Adding live message:", message);
                 setChatMessages(prev => { if (prev.some(m => m.id === message.id)) { return prev; } const updated = [...prev, message]; return updated.slice(-HISTORY_LIMIT); });
                 if (chatError) setChatError(null);
            }
        });
        socket.on('chat_error', (errorData) => { console.error("ViewPage: Chat Error from server:", errorData.error); setChatError(errorData.error || 'An unknown chat error occurred.'); });
        return () => { if (chatSocketRef.current) { chatSocketRef.current.disconnect(); chatSocketRef.current = null; } setChatSocketConnected(false); setChatError(null); historyLoadedRef.current = false; };
    }, [chatServerUrl, streamId, fetchChatHistory]);


    // --- Stale Video Frame Check (Unchanged) ---
    useEffect(() => {
        // ... (Stale frame check logic remains the same) ...
         let staleTimer = null;
         if (videoSocketConnected && currentFrame) {
           lastFrameTimestampRef.current = Date.now();
           staleTimer = setInterval(() => { const timeSince = Date.now() - lastFrameTimestampRef.current; if (timeSince > STALE_FRAME_THRESHOLD_MS) { if (!videoError?.includes("paused")) { setVideoError("Stream seems paused..."); } setCalculatedFps(0); } }, STALE_CHECK_INTERVAL_MS);
         } else { if (staleTimer) clearInterval(staleTimer); }
         return () => { if (staleTimer) clearInterval(staleTimer); };
    }, [videoSocketConnected, currentFrame, videoError]);


    // --- Handler for Sending Chat Messages (Unchanged) ---
    const handleSendMessage = useCallback((messageText) => {
        // ... (handleSendMessage logic remains the same - already sends streamId) ...
        if (chatSocketRef.current && chatSocketConnected && messageText.trim() && streamId) {
            console.log(`ViewPage: Sending chat message for stream ${streamId}:`, messageText);
            chatSocketRef.current.emit('send_chat_message', { message: messageText, stream_id: streamId });
            setChatError(null);
        } else if (!chatSocketConnected) { setChatError('Cannot send message: Chat not connected.'); }
        else if (!streamId) { setChatError('Cannot send message: Stream ID is missing.'); }
    }, [chatSocketConnected, streamId]);


    // --- Render Logic (Add Viewer Count) ---
    return (
        <div className="view-page">
            <h1>Live Stream Viewer {streamId ? `(${streamId})` : ''}</h1>

            <div className="status-bar">
                {/* Video Status */}
                <span>Video: <span className={videoSocketConnected ? 'status-ok' : 'status-error'}>{videoSocketConnected ? 'Connected' : 'Disconnected'}</span></span>
                {/* --- Display Viewer Count --- */}
                {videoSocketConnected && <span> | Viewers: {viewerCount}</span>}
                {/* --- End Viewer Count --- */}
                {videoSocketConnected && currentFrame && !videoError && <span> | Status: <span className='status-ok'>Receiving</span></span>}
                {videoSocketConnected && !currentFrame && !videoError && <span> | Status: <span className='status-warn'>Waiting...</span></span>}
                {videoSocketConnected && currentFrame && !videoError && <span> | Latency: {latency.toFixed(1)} ms</span>}
                {videoSocketConnected && currentFrame && !videoError && <span> | Recv FPS: {calculatedFps}</span>}
                <span style={{ margin: '0 10px' }}>||</span>
                <span>Chat: <span className={chatSocketConnected ? 'status-ok' : 'status-error'}>{chatSocketConnected ? 'Connected' : 'Disconnected'}</span></span>
                {isLoadingHistory && <span className="status-warn"> | Loading History...</span>}
                {videoError && <span className="error-message"> | Video Status: {videoError}</span>}
            </div>

            <div className="content-area">
                <div className="video-container">
                    {/* ... (Video display remains the same) ... */}
                    <h2>Video Feed</h2>
                     <div className="video-display">
                        {currentFrame && videoSocketConnected ? (<img src={currentFrame} alt="Live Stream" />) : (<div className="video-placeholder">{videoError ? videoError : (videoSocketConnected ? "Waiting for stream..." : "Video Disconnected")}</div>)}
                    </div>
                </div>

                <ChatInterface
                    messages={chatMessages}
                    isConnected={chatSocketConnected}
                    onSendMessage={handleSendMessage}
                    chatError={chatError}
                />
            </div>
        </div>
    );
}

export default ViewPage;