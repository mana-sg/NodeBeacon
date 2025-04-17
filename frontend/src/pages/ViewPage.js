// src/pages/ViewPage.js (Updated for History & stream_id)
import React, { useState, useEffect, useRef, useCallback } from 'react';
import io from 'socket.io-client';
// Import useParams to get URL parameters
import { useParams } from 'react-router-dom';
import ChatInterface from '../components/ChatInterface';
import './ViewPage.css';
const HISTORY_LIMIT = 20
const FPS_UPDATE_INTERVAL_MS = 1000;
const STALE_FRAME_THRESHOLD_MS = 5000;
const STALE_CHECK_INTERVAL_MS = 2000;

// Note: App.js now passes videoServerUrl and chatServerUrl
function ViewPage({ videoServerUrl, chatServerUrl }) {
    // --- Get streamId from URL ---
    const { streamId } = useParams(); // Get the :streamId part from the route

    // --- State ---
    const [videoSocketConnected, setVideoSocketConnected] = useState(false);
    const [chatSocketConnected, setChatSocketConnected] = useState(false);
    const [currentFrame, setCurrentFrame] = useState(null);
    const [latency, setLatency] = useState(0);
    const [chatMessages, setChatMessages] = useState([]); // Holds combined history + live
    const [calculatedFps, setCalculatedFps] = useState(0);
    const [videoError, setVideoError] = useState(null);
    const [chatError, setChatError] = useState(null);
    const [isLoadingHistory, setIsLoadingHistory] = useState(false); // For loading indicator

    // --- Refs ---
    const videoSocketRef = useRef(null);
    const chatSocketRef = useRef(null);
    const frameCountRef = useRef(0);
    const lastFpsUpdateTimeRef = useRef(Date.now());
    const lastFrameTimestampRef = useRef(0);
    // Ref to track if history has been loaded to prevent multiple fetches
    const historyLoadedRef = useRef(false);

    // --- Fetch Chat History Function ---
    const fetchChatHistory = useCallback(async () => {
        if (!streamId || historyLoadedRef.current) {
            // Don't fetch if no streamId or already loaded
            return;
        }
        console.log(`ViewPage: Fetching chat history for stream: ${streamId}`);
        setIsLoadingHistory(true);
        setChatError(null); // Clear previous errors
        historyLoadedRef.current = true; // Mark as attempting load

        try {
            // Construct the history endpoint URL
            // Ensure chatServerUrl doesn't have trailing slash if using template literal
            const historyUrl = `${chatServerUrl.replace(/\/$/, '')}/chat/history/${streamId}`;
            const response = await fetch(historyUrl);

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
            }

            const history = await response.json();
            console.log(`ViewPage: Received ${history.length} historical messages.`);

            // Ensure history messages have a compatible structure if needed
            // Add 'id' based on DB id if not already present from query
            const formattedHistory = history.map(msg => ({
                ...msg,
                id: msg.id || `${msg.timestamp}-${msg.sender}` // Ensure unique key
            }));

            setChatMessages(formattedHistory); // Replace state with history

        } catch (error) {
            console.error("ViewPage: Failed to fetch chat history:", error);
            setChatError(`Failed to load chat history: ${error.message}`);
            historyLoadedRef.current = false; // Allow retry on next connection attempt
        } finally {
            setIsLoadingHistory(false);
        }
    }, [streamId, chatServerUrl]); // Dependencies for the fetch function

    // --- Video Socket Connection (Largely Unchanged) ---
    useEffect(() => {
        // ... (Video socket connection logic remains the same as previous version) ...
        if (!videoServerUrl) return;
        if (videoSocketRef.current) {
            videoSocketRef.current.disconnect();
            videoSocketRef.current = null;
        }
        console.log(`ViewPage: Attempting to connect to Video Server at ${videoServerUrl}`);
        videoSocketRef.current = io(videoServerUrl, { /* ... options ... */ });
        const socket = videoSocketRef.current;
        socket.on('connect', () => { /* ... set state ... */ setVideoSocketConnected(true); setVideoError(null); });
        socket.on('disconnect', (reason) => { /* ... set state ... */ setVideoSocketConnected(false); setVideoError(`Video Disconnected: ${reason}`); });
        socket.on('connect_error', (error) => { /* ... set state ... */ setVideoSocketConnected(false); setVideoError(`Video Connection Error: ${error.message || 'Unknown error'}`); });
        socket.on('video_frame', (data) => { /* ... process frame, latency, FPS ... */
             if (!data || !data.image) return;
             setCurrentFrame(data.image);
             const receiveTime = Date.now();
             lastFrameTimestampRef.current = receiveTime;
             // Latency...
             if (typeof data.latency === 'number') setLatency(data.latency);
             else if (typeof data.timestamp === 'number') setLatency((receiveTime / 1000 - data.timestamp) * 1000);
             // FPS...
             frameCountRef.current++;
             const timeNow = Date.now();
             const timeDiff = timeNow - lastFpsUpdateTimeRef.current;
             if (timeDiff >= FPS_UPDATE_INTERVAL_MS) {
               setCalculatedFps((frameCountRef.current / (timeDiff / 1000)).toFixed(1));
               frameCountRef.current = 0;
               lastFpsUpdateTimeRef.current = timeNow;
             }
             if (videoError) setVideoError(null);
        });
        return () => { /* ... disconnect videoSocketRef ... */
            if (videoSocketRef.current) { videoSocketRef.current.disconnect(); videoSocketRef.current = null; }
            setVideoSocketConnected(false); setCurrentFrame(null); setCalculatedFps(0); setLatency(0); setVideoError(null);
         };
    }, [videoServerUrl]);


    // --- Chat Socket Connection & History Fetch ---
    useEffect(() => {
        if (!chatServerUrl) return;
         if (chatSocketRef.current) {
             chatSocketRef.current.disconnect();
             chatSocketRef.current = null;
         }

        console.log(`ViewPage: Attempting to connect to Chat Server at ${chatServerUrl}`);
        historyLoadedRef.current = false; // Reset history loaded flag on new connection attempt
        chatSocketRef.current = io(chatServerUrl, { /* ... options ... */ });
        const socket = chatSocketRef.current;

        socket.on('connect', () => {
          console.log('ViewPage: Connected to Chat Server', socket.id);
          setChatSocketConnected(true);
          setChatError(null);
          // --- FETCH HISTORY ON CONNECT ---
          fetchChatHistory();
        });

        socket.on('disconnect', (reason) => {
          console.log('ViewPage: Disconnected from Chat Server:', reason);
          setChatSocketConnected(false);
          setChatError(`Chat Disconnected: ${reason}`);
          historyLoadedRef.current = false; // Allow history fetch on reconnect
        });

        socket.on('connect_error', (error) => {
          console.error('ViewPage: Chat connection error:', error);
          setChatSocketConnected(false);
          setChatError(`Chat Connection Error: ${error.message || 'Unknown error'}`);
          historyLoadedRef.current = false; // Allow history fetch on reconnect attempt
        });

        socket.on('new_chat_message', (message) => {
            // --- FILTER INCOMING LIVE MESSAGES ---
            if (!message || typeof message !== 'object' || !message.id || !message.stream_id ) {
                console.warn("Received invalid live chat message structure:", message);
                return;
            }
            // Only add if the message's stream_id matches the current page's streamId
            if (message.stream_id === streamId) {
                console.log("Adding live message matching streamId:", message);
                setChatMessages(prevMessages => {
                    // Check for duplicates again just in case
                    if (prevMessages.some(m => m.id === message.id)) {
                        return prevMessages;
                    }
                    const updatedMessages = [...prevMessages, message];
                    return updatedMessages.slice(-HISTORY_LIMIT); // Keep total messages bounded
                });
                if (chatError) setChatError(null); // Clear error if live messages arrive
            } else {
                 // console.log("Ignoring message for different stream:", message.stream_id); // Optional log
            }
        });

        socket.on('chat_error', (errorData) => {
           console.error("ViewPage: Chat Error from server:", errorData.error);
           setChatError(errorData.error || 'An unknown chat error occurred.');
        });

        // Cleanup
        return () => {
            if (chatSocketRef.current) { chatSocketRef.current.disconnect(); chatSocketRef.current = null; }
            setChatSocketConnected(false); setChatError(null); historyLoadedRef.current = false;
            // Decide if you want to clear chatMessages on disconnect here, or keep them until next successful load
            // setChatMessages([]);
        };
    }, [chatServerUrl, streamId, fetchChatHistory]); // Add streamId and fetchChatHistory to dependencies


    // --- Stale Video Frame Check (Unchanged) ---
    useEffect(() => {
        // ... (Stale frame check logic remains the same) ...
         let staleTimer = null;
         if (videoSocketConnected && currentFrame) {
           lastFrameTimestampRef.current = Date.now();
           staleTimer = setInterval(() => {
             const timeSinceLastFrame = Date.now() - lastFrameTimestampRef.current;
             if (timeSinceLastFrame > STALE_FRAME_THRESHOLD_MS) {
               if (!videoError?.includes("paused")) { setVideoError("Stream seems paused..."); }
               setCalculatedFps(0);
             }
           }, STALE_CHECK_INTERVAL_MS);
         } else { if (staleTimer) clearInterval(staleTimer); }
         return () => { if (staleTimer) clearInterval(staleTimer); };
    }, [videoSocketConnected, currentFrame, videoError]);


    // --- Handler for Sending Chat Messages ---
    const handleSendMessage = useCallback((messageText) => {
        // Ensure chat socket is connected AND streamId is available
        if (chatSocketRef.current && chatSocketConnected && messageText.trim() && streamId) {
            console.log(`ViewPage: Sending chat message for stream ${streamId}:`, messageText);
            // --- SEND stream_id WITH MESSAGE ---
            chatSocketRef.current.emit('send_chat_message', {
                message: messageText,
                stream_id: streamId // Include the stream ID
            });
            setChatError(null);
        } else if (!chatSocketConnected) {
            setChatError('Cannot send message: Chat not connected.');
        } else if (!streamId) {
             setChatError('Cannot send message: Stream ID is missing.');
        }
    }, [chatSocketConnected, streamId]); // Add streamId dependency


    // --- Render Logic ---
    return (
        <div className="view-page">
            {/* Display stream ID if available */}
            <h1>Live Stream Viewer {streamId ? `(${streamId})` : ''}</h1>

            <div className="status-bar">
                {/* ... (Video status remains the same) ... */}
                 <span>Video: <span className={videoSocketConnected ? 'status-ok' : 'status-error'}>{videoSocketConnected ? 'Connected' : 'Disconnected'}</span></span>
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

                {/* Pass chat state down */}
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