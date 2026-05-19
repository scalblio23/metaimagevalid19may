import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { z } from "zod";
import axios from "axios";
import FormData from "form-data";
import express from "express";
import cors from "cors";
import { Readable } from "stream";
import path from "path";
// ─── Configuration ───────────────────────────────────────────────────────────
const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || "";
const META_AD_ACCOUNT_ID = process.env.META_AD_ACCOUNT_ID || ""; // format: act_XXXXXXXXX
const META_APP_ID = process.env.META_APP_ID || "";
const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0";
const GRAPH_API_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;
const PORT = parseInt(process.env.PORT || "3000", 10);
const AUTH_TOKEN = process.env.AUTH_TOKEN || ""; // Optional: secure your MCP endpoint
// ─── Helper Functions ────────────────────────────────────────────────────────
async function downloadFile(url) {
    const response = await axios.get(url, {
        responseType: "arraybuffer",
        maxContentLength: 4 * 1024 * 1024 * 1024, // 4GB
        timeout: 600000, // 10 min
    });
    const contentType = String(response.headers["content-type"] || "application/octet-stream");
    const urlPath = new URL(url).pathname;
    const fileName = path.basename(urlPath) || "upload";
    return {
        buffer: Buffer.from(response.data),
        fileName,
        mimeType: contentType.split(";")[0].trim(),
    };
}
function validateImageMime(mimeType, fileName) {
    const validMimes = ["image/jpeg", "image/jpg", "image/png", "image/gif", "image/bmp", "image/tiff", "image/webp"];
    const validExts = [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"];
    const ext = path.extname(fileName).toLowerCase();
    if (!validMimes.includes(mimeType) && !validExts.includes(ext)) {
        throw new Error(`Invalid image format. MIME: "${mimeType}", extension: "${ext}". Supported: jpg, png, gif, bmp, tiff, webp`);
    }
}
function validateVideoMime(mimeType, fileName) {
    const validMimes = ["video/mp4", "video/quicktime", "video/x-msvideo", "video/x-matroska", "video/webm", "image/gif"];
    const validExts = [".mp4", ".mov", ".avi", ".mkv", ".webm", ".gif"];
    const ext = path.extname(fileName).toLowerCase();
    if (!validMimes.includes(mimeType) && !validExts.includes(ext)) {
        throw new Error(`Invalid video format. MIME: "${mimeType}", extension: "${ext}". Supported: mp4, mov, avi, mkv, webm, gif`);
    }
}
// ─── Meta API Functions ──────────────────────────────────────────────────────
async function uploadAdImageFromUrl(imageUrl, adAccountId) {
    // Try direct URL upload first (Meta supports this natively for images)
    try {
        const response = await axios.post(`${GRAPH_API_BASE}/${adAccountId}/adimages`, null, {
            params: {
                url: imageUrl,
                access_token: META_ACCESS_TOKEN,
            },
        });
        const images = response.data.images;
        const firstKey = Object.keys(images)[0];
        const imageData = images[firstKey];
        return {
            hash: imageData?.hash || "unknown",
            url: imageData?.url || imageData?.url_128 || "",
            name: firstKey || "uploaded_image",
        };
    }
    catch (directError) {
        // If direct URL fails, download and re-upload as multipart
        const { buffer, fileName, mimeType } = await downloadFile(imageUrl);
        validateImageMime(mimeType, fileName);
        if (buffer.length > 30 * 1024 * 1024) {
            throw new Error(`Image size ${(buffer.length / 1024 / 1024).toFixed(1)}MB exceeds Meta's 30MB limit`);
        }
        const form = new FormData();
        form.append("filename", Readable.from(buffer), {
            filename: fileName,
            contentType: mimeType,
        });
        form.append("access_token", META_ACCESS_TOKEN);
        const response = await axios.post(`${GRAPH_API_BASE}/${adAccountId}/adimages`, form, {
            headers: { ...form.getHeaders() },
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
        });
        const images = response.data.images;
        const firstKey = Object.keys(images)[0];
        const imageData = images[firstKey];
        return {
            hash: imageData?.hash || "unknown",
            url: imageData?.url || imageData?.url_128 || "",
            name: firstKey || fileName,
        };
    }
}
async function uploadAdVideoFromUrl(videoUrl, adAccountId, title, description) {
    const { buffer, fileName, mimeType } = await downloadFile(videoUrl);
    validateVideoMime(mimeType, fileName);
    if (buffer.length > 4 * 1024 * 1024 * 1024) {
        throw new Error(`Video size exceeds Meta's 4GB limit`);
    }
    const videoTitle = title || fileName;
    // For videos under 1GB, use single-request upload
    if (buffer.length < 1 * 1024 * 1024 * 1024) {
        const form = new FormData();
        form.append("source", Readable.from(buffer), {
            filename: fileName,
            contentType: mimeType || "video/mp4",
        });
        form.append("title", videoTitle);
        if (description)
            form.append("description", description);
        form.append("access_token", META_ACCESS_TOKEN);
        const response = await axios.post(`${GRAPH_API_BASE}/${adAccountId}/advideos`, form, {
            headers: { ...form.getHeaders() },
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            timeout: 600000,
        });
        return {
            videoId: response.data.id,
            title: videoTitle,
            uploadStatus: "processing",
        };
    }
    // For videos over 1GB, use chunked resumable upload
    const startResponse = await axios.post(`${GRAPH_API_BASE}/${adAccountId}/advideos`, {
        upload_phase: "start",
        file_size: buffer.length,
        access_token: META_ACCESS_TOKEN,
    });
    const { upload_session_id, video_id } = startResponse.data;
    const chunkSize = 50 * 1024 * 1024; // 50MB chunks
    let startOffset = 0;
    while (startOffset < buffer.length) {
        const currentChunkSize = Math.min(chunkSize, buffer.length - startOffset);
        const chunk = buffer.subarray(startOffset, startOffset + currentChunkSize);
        const chunkForm = new FormData();
        chunkForm.append("upload_phase", "transfer");
        chunkForm.append("upload_session_id", upload_session_id);
        chunkForm.append("start_offset", startOffset.toString());
        chunkForm.append("video_file_chunk", Readable.from(chunk), {
            filename: `chunk_${startOffset}`,
            contentType: "application/octet-stream",
        });
        chunkForm.append("access_token", META_ACCESS_TOKEN);
        const chunkResponse = await axios.post(`${GRAPH_API_BASE}/${adAccountId}/advideos`, chunkForm, {
            headers: { ...chunkForm.getHeaders() },
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            timeout: 300000,
        });
        startOffset = parseInt(chunkResponse.data.start_offset, 10);
    }
    // Finish upload
    await axios.post(`${GRAPH_API_BASE}/${adAccountId}/advideos`, {
        upload_phase: "finish",
        upload_session_id,
        title: videoTitle,
        description: description || "",
        access_token: META_ACCESS_TOKEN,
    });
    return {
        videoId: video_id,
        title: videoTitle,
        uploadStatus: "processing",
    };
}
async function checkVideoStatus(videoId) {
    const response = await axios.get(`${GRAPH_API_BASE}/${videoId}`, {
        params: {
            fields: "id,title,status,length,thumbnails",
            access_token: META_ACCESS_TOKEN,
        },
    });
    return {
        id: response.data.id,
        status: response.data.status?.video_status || "unknown",
        title: response.data.title || "",
        length: response.data.length || 0,
        thumbnails: response.data.thumbnails?.data?.map((t) => t.uri) || [],
    };
}
async function listAdImages(adAccountId, limit = 25) {
    const response = await axios.get(`${GRAPH_API_BASE}/${adAccountId}/adimages`, {
        params: {
            fields: "hash,name,url_128,width,height,created_time",
            limit,
            access_token: META_ACCESS_TOKEN,
        },
    });
    return response.data.data;
}
// ─── MCP Server Setup ────────────────────────────────────────────────────────
function createMcpServer() {
    const server = new McpServer({
        name: "meta-ads-media",
        version: "1.0.0",
    });
    // Tool: Upload Image from URL
    server.tool("upload_ad_image", "Upload an image to Meta Ads Library from a public URL. Returns the image hash needed for creating ad creatives. Supports jpg, png, gif, bmp, tiff, webp.", {
        image_url: z
            .string()
            .describe("Public URL of the image to upload to Meta Ads"),
        ad_account_id: z
            .string()
            .optional()
            .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Uses server default if not provided."),
    }, async ({ image_url, ad_account_id }) => {
        try {
            const accountId = ad_account_id || META_AD_ACCOUNT_ID;
            if (!accountId) {
                return { content: [{ type: "text", text: "Error: No ad account ID configured. Set META_AD_ACCOUNT_ID on the server or pass ad_account_id." }] };
            }
            if (!META_ACCESS_TOKEN) {
                return { content: [{ type: "text", text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
            }
            const result = await uploadAdImageFromUrl(image_url, accountId);
            return {
                content: [{
                        type: "text",
                        text: JSON.stringify({
                            success: true,
                            image_hash: result.hash,
                            image_url: result.url,
                            file_name: result.name,
                            message: `Image uploaded successfully. Use image_hash "${result.hash}" when creating ad creatives.`,
                        }, null, 2),
                    }],
            };
        }
        catch (error) {
            const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
            return { content: [{ type: "text", text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
        }
    });
    // Tool: Upload Video from URL
    server.tool("upload_ad_video", "Upload a video to Meta Ads Library from a public URL. Supports files up to 4GB with automatic chunked upload. Returns the video ID needed for creating ad creatives.", {
        video_url: z
            .string()
            .describe("Public URL of the video file to upload (mp4, mov, avi, mkv, webm)"),
        title: z
            .string()
            .optional()
            .describe("Title for the video in Meta Ads Library"),
        description: z
            .string()
            .optional()
            .describe("Description for the video"),
        ad_account_id: z
            .string()
            .optional()
            .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Uses server default if not provided."),
    }, async ({ video_url, title, description, ad_account_id }) => {
        try {
            const accountId = ad_account_id || META_AD_ACCOUNT_ID;
            if (!accountId) {
                return { content: [{ type: "text", text: "Error: No ad account ID configured. Set META_AD_ACCOUNT_ID on the server or pass ad_account_id." }] };
            }
            if (!META_ACCESS_TOKEN) {
                return { content: [{ type: "text", text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
            }
            const result = await uploadAdVideoFromUrl(video_url, accountId, title, description);
            return {
                content: [{
                        type: "text",
                        text: JSON.stringify({
                            success: true,
                            video_id: result.videoId,
                            title: result.title,
                            upload_status: result.uploadStatus,
                            message: `Video uploaded successfully. Video ID: "${result.videoId}". Video is processing — use check_video_status to verify it's ready before creating ad creatives.`,
                        }, null, 2),
                    }],
            };
        }
        catch (error) {
            const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
            return { content: [{ type: "text", text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
        }
    });
    // Tool: Check Video Status
    server.tool("check_video_status", "Check the processing status of a previously uploaded video. Videos need time to process on Meta's servers before they can be used in ads.", {
        video_id: z
            .string()
            .describe("The video ID returned from upload_ad_video"),
    }, async ({ video_id }) => {
        try {
            if (!META_ACCESS_TOKEN) {
                return { content: [{ type: "text", text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
            }
            const result = await checkVideoStatus(video_id);
            return {
                content: [{
                        type: "text",
                        text: JSON.stringify({
                            success: true,
                            video_id: result.id,
                            status: result.status,
                            title: result.title,
                            length_seconds: result.length,
                            thumbnails: result.thumbnails,
                            ready: result.status === "ready",
                            message: result.status === "ready"
                                ? "Video is ready to use in ad creatives."
                                : `Video is still ${result.status}. Check again in a moment.`,
                        }, null, 2),
                    }],
            };
        }
        catch (error) {
            const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
            return { content: [{ type: "text", text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
        }
    });
    // Tool: List Ad Images
    server.tool("list_ad_images", "List existing images in the Meta Ad Account's image library. Useful for checking what's already uploaded or finding image hashes.", {
        ad_account_id: z
            .string()
            .optional()
            .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Uses server default if not provided."),
        limit: z
            .number()
            .optional()
            .describe("Number of images to return (default: 25, max: 100)"),
    }, async ({ ad_account_id, limit }) => {
        try {
            const accountId = ad_account_id || META_AD_ACCOUNT_ID;
            if (!accountId || !META_ACCESS_TOKEN) {
                return { content: [{ type: "text", text: "Error: Missing ad account ID or access token." }] };
            }
            const images = await listAdImages(accountId, limit || 25);
            return {
                content: [{
                        type: "text",
                        text: JSON.stringify({ success: true, images, count: images.length }, null, 2),
                    }],
            };
        }
        catch (error) {
            const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
            return { content: [{ type: "text", text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
        }
    });
    return server;
}
// ─── Express + SSE Transport ─────────────────────────────────────────────────
const app = express();
app.use(cors());
// Health check endpoint
app.get("/health", (_req, res) => {
    res.json({ status: "ok", server: "meta-ads-media-mcp", version: "1.0.0" });
});
// Store active transports
const transports = new Map();
// SSE endpoint - client connects here to establish the MCP session
app.get("/sse", async (req, res) => {
    // Optional auth check
    if (AUTH_TOKEN) {
        const authHeader = req.headers.authorization;
        if (!authHeader || authHeader !== `Bearer ${AUTH_TOKEN}`) {
            res.status(401).json({ error: "Unauthorized" });
            return;
        }
    }
    const transport = new SSEServerTransport("/messages", res);
    const sessionId = transport.sessionId;
    transports.set(sessionId, transport);
    const server = createMcpServer();
    res.on("close", () => {
        transports.delete(sessionId);
    });
    await server.connect(transport);
});
// Messages endpoint - client sends MCP messages here
app.post("/messages", async (req, res) => {
    const sessionId = req.query.sessionId;
    const transport = transports.get(sessionId);
    if (!transport) {
        res.status(404).json({ error: "Session not found" });
        return;
    }
    await transport.handlePostMessage(req, res);
});
// Start the server
app.listen(PORT, "0.0.0.0", () => {
    console.log(`Meta Ads Media MCP Server running on port ${PORT}`);
    console.log(`SSE endpoint: http://0.0.0.0:${PORT}/sse`);
    console.log(`Health check: http://0.0.0.0:${PORT}/health`);
});
//# sourceMappingURL=index.js.map