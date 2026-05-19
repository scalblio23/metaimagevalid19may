import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import axios from "axios";
import FormData from "form-data";
import express from "express";
import cors from "cors";
import { Readable } from "stream";
import path from "path";
import type { Request, Response } from "express";

// ─── Configuration ───────────────────────────────────────────────────────────

const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || "";
const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || "v25.0";
const GRAPH_API_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;
const PORT = parseInt(process.env.PORT || "3000", 10);

// ─── Helper Functions ────────────────────────────────────────────────────────

async function downloadFile(url: string): Promise<{ buffer: Buffer; fileName: string; mimeType: string }> {
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

function mimeFromExtension(fileName: string): string {
  const ext = path.extname(fileName).toLowerCase();
  const map: Record<string, string> = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".tiff": "image/tiff",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".mov": "video/quicktime",
    ".avi": "video/x-msvideo",
    ".mkv": "video/x-matroska",
    ".webm": "video/webm",
  };
  return map[ext] || "application/octet-stream";
}

// ─── Meta API Functions ──────────────────────────────────────────────────────

async function fetchAdAccounts(): Promise<Array<{ id: string; name: string; account_status: number; currency: string; business_name: string }>> {
  const response = await axios.get(`${GRAPH_API_BASE}/me/adaccounts`, {
    params: {
      fields: "id,name,account_status,currency,business_name",
      limit: 100,
      access_token: META_ACCESS_TOKEN,
    },
  });
  return response.data.data;
}

async function uploadImageBuffer(
  buffer: Buffer,
  fileName: string,
  mimeType: string,
  adAccountId: string
): Promise<{ hash: string; url: string; name: string }> {
  const form = new FormData();
  form.append("filename", Readable.from(buffer), {
    filename: fileName,
    contentType: mimeType,
  });
  form.append("access_token", META_ACCESS_TOKEN);

  const response = await axios.post(
    `${GRAPH_API_BASE}/${adAccountId}/adimages`,
    form,
    {
      headers: { ...form.getHeaders() },
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
    }
  );

  const images = response.data.images;
  const firstKey = Object.keys(images)[0];
  const imageData = images[firstKey];

  return {
    hash: imageData?.hash || "unknown",
    url: imageData?.url || imageData?.url_128 || "",
    name: firstKey || fileName,
  };
}

async function uploadAdImageFromUrl(
  imageUrl: string,
  adAccountId: string
): Promise<{ hash: string; url: string; name: string }> {
  try {
    const response = await axios.post(
      `${GRAPH_API_BASE}/${adAccountId}/adimages`,
      null,
      {
        params: {
          url: imageUrl,
          access_token: META_ACCESS_TOKEN,
        },
      }
    );

    const images = response.data.images;
    const firstKey = Object.keys(images)[0];
    const imageData = images[firstKey];

    return {
      hash: imageData?.hash || "unknown",
      url: imageData?.url || imageData?.url_128 || "",
      name: firstKey || "uploaded_image",
    };
  } catch (directError: any) {
    const { buffer, fileName, mimeType } = await downloadFile(imageUrl);

    if (buffer.length > 30 * 1024 * 1024) {
      throw new Error(`Image size ${(buffer.length / 1024 / 1024).toFixed(1)}MB exceeds Meta's 30MB limit`);
    }

    return uploadImageBuffer(buffer, fileName, mimeType, adAccountId);
  }
}

async function uploadVideoBuffer(
  buffer: Buffer,
  fileName: string,
  mimeType: string,
  adAccountId: string,
  title?: string,
  description?: string
): Promise<{ videoId: string; title: string; uploadStatus: string }> {
  const videoTitle = title || fileName;

  if (buffer.length < 1 * 1024 * 1024 * 1024) {
    const form = new FormData();
    form.append("source", Readable.from(buffer), {
      filename: fileName,
      contentType: mimeType || "video/mp4",
    });
    form.append("title", videoTitle);
    if (description) form.append("description", description);
    form.append("access_token", META_ACCESS_TOKEN);

    const response = await axios.post(
      `${GRAPH_API_BASE}/${adAccountId}/advideos`,
      form,
      {
        headers: { ...form.getHeaders() },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 600000,
      }
    );

    return {
      videoId: response.data.id,
      title: videoTitle,
      uploadStatus: "processing",
    };
  }

  // Chunked upload for large videos
  const startResponse = await axios.post(
    `${GRAPH_API_BASE}/${adAccountId}/advideos`,
    {
      upload_phase: "start",
      file_size: buffer.length,
      access_token: META_ACCESS_TOKEN,
    }
  );

  const { upload_session_id, video_id } = startResponse.data;
  const chunkSize = 50 * 1024 * 1024;
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

    const chunkResponse = await axios.post(
      `${GRAPH_API_BASE}/${adAccountId}/advideos`,
      chunkForm,
      {
        headers: { ...chunkForm.getHeaders() },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 300000,
      }
    );

    startOffset = parseInt(chunkResponse.data.start_offset, 10);
  }

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

async function uploadAdVideoFromUrl(
  videoUrl: string,
  adAccountId: string,
  title?: string,
  description?: string
): Promise<{ videoId: string; title: string; uploadStatus: string }> {
  const { buffer, fileName, mimeType } = await downloadFile(videoUrl);

  if (buffer.length > 4 * 1024 * 1024 * 1024) {
    throw new Error(`Video size exceeds Meta's 4GB limit`);
  }

  return uploadVideoBuffer(buffer, fileName, mimeType, adAccountId, title, description);
}

async function checkVideoStatus(videoId: string): Promise<{
  id: string;
  status: string;
  title: string;
  length: number;
  thumbnails: string[];
}> {
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
    thumbnails: response.data.thumbnails?.data?.map((t: any) => t.uri) || [],
  };
}

async function listAdImages(adAccountId: string, limit: number = 25) {
  const response = await axios.get(`${GRAPH_API_BASE}/${adAccountId}/adimages`, {
    params: {
      fields: "hash,name,url_128,width,height,created_time",
      limit,
      access_token: META_ACCESS_TOKEN,
    },
  });
  return response.data.data;
}

// ─── Meta Campaign/Ad Management Functions ─────────────────────────────────

async function listCampaigns(adAccountId: string, limit: number = 25) {
  const response = await axios.get(`${GRAPH_API_BASE}/${adAccountId}/campaigns`, {
    params: {
      fields: "id,name,status,objective,daily_budget,lifetime_budget,start_time,stop_time,created_time",
      limit,
      access_token: META_ACCESS_TOKEN,
    },
  });
  return response.data.data;
}

async function listAdSets(adAccountId: string, campaignId?: string, limit: number = 25) {
  const endpoint = campaignId
    ? `${GRAPH_API_BASE}/${campaignId}/adsets`
    : `${GRAPH_API_BASE}/${adAccountId}/adsets`;

  const response = await axios.get(endpoint, {
    params: {
      fields: "id,name,status,campaign_id,daily_budget,lifetime_budget,targeting,optimization_goal,billing_event,start_time,end_time",
      limit,
      access_token: META_ACCESS_TOKEN,
    },
  });
  return response.data.data;
}

async function listAds(adAccountId: string, adSetId?: string, limit: number = 25) {
  const endpoint = adSetId
    ? `${GRAPH_API_BASE}/${adSetId}/ads`
    : `${GRAPH_API_BASE}/${adAccountId}/ads`;

  const response = await axios.get(endpoint, {
    params: {
      fields: "id,name,status,adset_id,creative{id,name,title,body,image_hash,image_url,thumbnail_url,object_story_spec}",
      limit,
      access_token: META_ACCESS_TOKEN,
    },
  });
  return response.data.data;
}

async function createAdCreative(
  adAccountId: string,
  name: string,
  objectStorySpec: any
): Promise<{ id: string; name: string }> {
  const response = await axios.post(
    `${GRAPH_API_BASE}/${adAccountId}/adcreatives`,
    {
      name,
      object_story_spec: objectStorySpec,
      access_token: META_ACCESS_TOKEN,
    }
  );
  return { id: response.data.id, name };
}

async function createAd(
  adAccountId: string,
  name: string,
  adSetId: string,
  creativeId: string,
  status: string = "PAUSED"
): Promise<{ id: string; name: string }> {
  const response = await axios.post(
    `${GRAPH_API_BASE}/${adAccountId}/ads`,
    {
      name,
      adset_id: adSetId,
      creative: { creative_id: creativeId },
      status,
      access_token: META_ACCESS_TOKEN,
    }
  );
  return { id: response.data.id, name };
}

async function getAdCreativeDetails(creativeId: string) {
  const response = await axios.get(`${GRAPH_API_BASE}/${creativeId}`, {
    params: {
      fields: "id,name,title,body,image_hash,image_url,object_story_spec,url_tags,asset_feed_spec",
      access_token: META_ACCESS_TOKEN,
    },
  });
  return response.data;
}

// ─── MCP Server Setup ────────────────────────────────────────────────────────

function createMcpServer(): McpServer {
  const server = new McpServer({
    name: "meta-ads-media",
    version: "2.0.0",
  });

  // Tool: List Ad Accounts
  server.tool(
    "list_ad_accounts",
    "List all Meta Ad Accounts accessible with the configured access token. Use this first to find the correct ad_account_id before uploading media.",
    {},
    async () => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const accounts = await fetchAdAccounts();

        const statusMap: Record<number, string> = {
          1: "ACTIVE",
          2: "DISABLED",
          3: "UNSETTLED",
          7: "PENDING_RISK_REVIEW",
          8: "PENDING_SETTLEMENT",
          9: "IN_GRACE_PERIOD",
          100: "PENDING_CLOSURE",
          101: "CLOSED",
          201: "ANY_ACTIVE",
          202: "ANY_CLOSED",
        };

        const formatted = accounts.map((a: any) => ({
          id: a.id,
          name: a.name || "Unnamed",
          status: statusMap[a.account_status] || `UNKNOWN (${a.account_status})`,
          currency: a.currency || "N/A",
          business_name: a.business_name || "N/A",
        }));

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              ad_accounts: formatted,
              count: formatted.length,
              message: "Use the 'id' field (e.g., act_XXXXXXXXX) as the ad_account_id parameter in other tools.",
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Upload Image from base64
  server.tool(
    "upload_ad_image_base64",
    "Upload an image to Meta Ads Library from base64-encoded data. Use this when you have the image content directly (e.g., from a file the user shared in chat). Send the FULL base64 string in one call — do NOT chunk it.",
    {
      base64_data: z
        .string()
        .describe("The complete base64-encoded image data (without the data:image/... prefix). Send it all in one string."),
      file_name: z
        .string()
        .describe("File name with extension (e.g., 'ad-creative.png', 'hero-image.jpg')"),
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Use list_ad_accounts to find available accounts."),
    },
    async ({ base64_data, file_name, ad_account_id }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }
        if (!ad_account_id) {
          return { content: [{ type: "text" as const, text: "Error: ad_account_id is required. Use list_ad_accounts to find available accounts." }] };
        }

        const cleanBase64 = base64_data.replace(/^data:[^;]+;base64,/, "");
        const buffer = Buffer.from(cleanBase64, "base64");

        if (buffer.length > 30 * 1024 * 1024) {
          return { content: [{ type: "text" as const, text: `Error: Image size ${(buffer.length / 1024 / 1024).toFixed(1)}MB exceeds Meta's 30MB limit.` }] };
        }

        const mimeType = mimeFromExtension(file_name);
        const result = await uploadImageBuffer(buffer, file_name, mimeType, ad_account_id);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              image_hash: result.hash,
              image_url: result.url,
              file_name: result.name,
              file_size_mb: (buffer.length / 1024 / 1024).toFixed(2),
              ad_account_id,
              message: `Image uploaded successfully. Use image_hash "${result.hash}" when creating ad creatives.`,
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Upload Video from base64
  server.tool(
    "upload_ad_video_base64",
    "Upload a video to Meta Ads Library from base64-encoded data. Use this when you have the video content directly. Send the FULL base64 string in one call — do NOT chunk it.",
    {
      base64_data: z
        .string()
        .describe("The complete base64-encoded video data (without the data:video/... prefix). Send it all in one string."),
      file_name: z
        .string()
        .describe("File name with extension (e.g., 'ad-video.mp4', 'promo.mov')"),
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Use list_ad_accounts to find available accounts."),
      title: z
        .string()
        .optional()
        .describe("Title for the video in Meta Ads Library"),
      description: z
        .string()
        .optional()
        .describe("Description for the video"),
    },
    async ({ base64_data, file_name, ad_account_id, title, description }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }
        if (!ad_account_id) {
          return { content: [{ type: "text" as const, text: "Error: ad_account_id is required. Use list_ad_accounts to find available accounts." }] };
        }

        const cleanBase64 = base64_data.replace(/^data:[^;]+;base64,/, "");
        const buffer = Buffer.from(cleanBase64, "base64");

        if (buffer.length > 4 * 1024 * 1024 * 1024) {
          return { content: [{ type: "text" as const, text: "Error: Video size exceeds Meta's 4GB limit." }] };
        }

        const mimeType = mimeFromExtension(file_name);
        const result = await uploadVideoBuffer(buffer, file_name, mimeType, ad_account_id, title, description);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              video_id: result.videoId,
              title: result.title,
              upload_status: result.uploadStatus,
              file_size_mb: (buffer.length / 1024 / 1024).toFixed(2),
              ad_account_id,
              message: `Video uploaded successfully. Video ID: "${result.videoId}". Video is processing — use check_video_status to verify it's ready.`,
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Upload Image from URL
  server.tool(
    "upload_ad_image_url",
    "Upload an image to Meta Ads Library from a public URL. Returns the image hash needed for creating ad creatives. Supports jpg, png, gif, bmp, tiff, webp.",
    {
      image_url: z
        .string()
        .describe("Public URL of the image to upload to Meta Ads"),
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Use list_ad_accounts to find available accounts."),
    },
    async ({ image_url, ad_account_id }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }
        if (!ad_account_id) {
          return { content: [{ type: "text" as const, text: "Error: ad_account_id is required. Use list_ad_accounts to find available accounts." }] };
        }

        const result = await uploadAdImageFromUrl(image_url, ad_account_id);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              image_hash: result.hash,
              image_url: result.url,
              file_name: result.name,
              ad_account_id,
              message: `Image uploaded successfully. Use image_hash "${result.hash}" when creating ad creatives.`,
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Upload Video from URL
  server.tool(
    "upload_ad_video_url",
    "Upload a video to Meta Ads Library from a public URL. Supports files up to 4GB with automatic chunked upload. Returns the video ID needed for creating ad creatives.",
    {
      video_url: z
        .string()
        .describe("Public URL of the video file to upload (mp4, mov, avi, mkv, webm)"),
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Use list_ad_accounts to find available accounts."),
      title: z
        .string()
        .optional()
        .describe("Title for the video in Meta Ads Library"),
      description: z
        .string()
        .optional()
        .describe("Description for the video"),
    },
    async ({ video_url, ad_account_id, title, description }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }
        if (!ad_account_id) {
          return { content: [{ type: "text" as const, text: "Error: ad_account_id is required. Use list_ad_accounts to find available accounts." }] };
        }

        const result = await uploadAdVideoFromUrl(video_url, ad_account_id, title, description);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              video_id: result.videoId,
              title: result.title,
              upload_status: result.uploadStatus,
              ad_account_id,
              message: `Video uploaded successfully. Video ID: "${result.videoId}". Video is processing — use check_video_status to verify it's ready.`,
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Check Video Status
  server.tool(
    "check_video_status",
    "Check the processing status of a previously uploaded video. Videos need time to process on Meta's servers before they can be used in ads.",
    {
      video_id: z
        .string()
        .describe("The video ID returned from upload_ad_video"),
    },
    async ({ video_id }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const result = await checkVideoStatus(video_id);

        return {
          content: [{
            type: "text" as const,
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
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: List Ad Images
  server.tool(
    "list_ad_images",
    "List existing images in a Meta Ad Account's image library. Useful for checking what's already uploaded or finding image hashes.",
    {
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Use list_ad_accounts to find available accounts."),
      limit: z
        .number()
        .optional()
        .describe("Number of images to return (default: 25, max: 100)"),
    },
    async ({ ad_account_id, limit }) => {
      try {
        if (!ad_account_id || !META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: Missing ad account ID or access token." }] };
        }

        const images = await listAdImages(ad_account_id, limit || 25);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({ success: true, images, count: images.length }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: List Campaigns
  server.tool(
    "list_campaigns",
    "List campaigns in a Meta Ad Account. Returns campaign IDs, names, status, objectives, and budgets. Use this to find the campaign you want to add ads to.",
    {
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX). Use list_ad_accounts to find available accounts."),
      limit: z
        .number()
        .optional()
        .describe("Number of campaigns to return (default: 25)"),
    },
    async ({ ad_account_id, limit }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const campaigns = await listCampaigns(ad_account_id, limit || 25);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({ success: true, campaigns, count: campaigns.length }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: List Ad Sets
  server.tool(
    "list_adsets",
    "List ad sets in a Meta Ad Account or within a specific campaign. Returns ad set IDs, names, status, targeting, and budgets.",
    {
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX)."),
      campaign_id: z
        .string()
        .optional()
        .describe("Optional: filter ad sets by campaign ID. If not provided, lists all ad sets in the account."),
      limit: z
        .number()
        .optional()
        .describe("Number of ad sets to return (default: 25)"),
    },
    async ({ ad_account_id, campaign_id, limit }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const adsets = await listAdSets(ad_account_id, campaign_id, limit || 25);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({ success: true, adsets, count: adsets.length }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: List Ads
  server.tool(
    "list_ads",
    "List ads in a Meta Ad Account or within a specific ad set. Returns ad IDs, names, status, and creative details. Useful for finding existing ads to duplicate or reference their copy.",
    {
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX)."),
      adset_id: z
        .string()
        .optional()
        .describe("Optional: filter ads by ad set ID. If not provided, lists all ads in the account."),
      limit: z
        .number()
        .optional()
        .describe("Number of ads to return (default: 25)"),
    },
    async ({ ad_account_id, adset_id, limit }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const ads = await listAds(ad_account_id, adset_id, limit || 25);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({ success: true, ads, count: ads.length }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Get Ad Creative Details
  server.tool(
    "get_ad_creative",
    "Get full details of an ad creative by ID. Returns the object_story_spec, image hash, copy text, and all other creative fields. Use this to inspect an existing ad's creative before duplicating it with a new image.",
    {
      creative_id: z
        .string()
        .describe("The creative ID to look up (from list_ads results)."),
    },
    async ({ creative_id }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const creative = await getAdCreativeDetails(creative_id);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({ success: true, creative }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Create Ad Creative
  server.tool(
    "create_ad_creative",
    "Create a new ad creative in Meta Ads. Requires an object_story_spec which defines the Facebook Page post (image, text, link, etc). Use get_ad_creative on an existing ad to see the format, then modify it with your new image_hash.",
    {
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX)."),
      name: z
        .string()
        .describe("Name for the creative (internal label, not shown to users)."),
      object_story_spec: z
        .string()
        .describe('JSON string of the object_story_spec. Must include page_id and the creative content (link_data with image_hash, message, link, call_to_action, etc). Example: {\"page_id\":\"123\",\"link_data\":{\"image_hash\":\"abc123\",\"link\":\"https://example.com\",\"message\":\"Ad text here\"}}'),
    },
    async ({ ad_account_id, name, object_story_spec }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        let parsedSpec: any;
        try {
          parsedSpec = JSON.parse(object_story_spec);
        } catch (e) {
          return { content: [{ type: "text" as const, text: "Error: object_story_spec must be valid JSON." }] };
        }

        const result = await createAdCreative(ad_account_id, name, parsedSpec);

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              creative_id: result.id,
              name: result.name,
              message: `Ad creative created successfully. Creative ID: "${result.id}". Use this ID with create_ad to attach it to an ad set.`,
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  // Tool: Create Ad
  server.tool(
    "create_ad",
    "Create a new ad within an ad set. Links a creative to an ad set so it starts delivering (or is paused). This is the final step to get an ad live.",
    {
      ad_account_id: z
        .string()
        .describe("Meta Ad Account ID (format: act_XXXXXXXXX)."),
      name: z
        .string()
        .describe("Name for the ad (internal label)."),
      adset_id: z
        .string()
        .describe("The ad set ID to place this ad in."),
      creative_id: z
        .string()
        .describe("The creative ID to use (from create_ad_creative)."),
      status: z
        .string()
        .optional()
        .describe("Ad status: ACTIVE or PAUSED (default: PAUSED). Set to ACTIVE to start delivering immediately."),
    },
    async ({ ad_account_id, name, adset_id, creative_id, status }) => {
      try {
        if (!META_ACCESS_TOKEN) {
          return { content: [{ type: "text" as const, text: "Error: META_ACCESS_TOKEN is not configured on the server." }] };
        }

        const result = await createAd(ad_account_id, name, adset_id, creative_id, status || "PAUSED");

        return {
          content: [{
            type: "text" as const,
            text: JSON.stringify({
              success: true,
              ad_id: result.id,
              name: result.name,
              status: status || "PAUSED",
              message: `Ad created successfully. Ad ID: "${result.id}". ${status === "ACTIVE" ? "Ad is now delivering." : "Ad is paused — set status to ACTIVE when ready to launch."}`,
            }, null, 2),
          }],
        };
      } catch (error: any) {
        const errorMsg = error.response?.data?.error?.message || error.message || "Unknown error";
        return { content: [{ type: "text" as const, text: JSON.stringify({ success: false, error: errorMsg }, null, 2) }] };
      }
    }
  );

  return server;
}

// ─── Express + Streamable HTTP Transport ────────────────────────────────────

const app = express();
app.use(cors());

// Health check endpoint
app.get("/health", (_req, res) => {
  res.json({ status: "ok", server: "meta-ads-media-mcp", version: "2.0.0", transport: "streamable-http" });
});

// MCP endpoint - Streamable HTTP (stateless mode)
// Each POST creates a fresh server+transport, processes the request, then closes.
// This avoids session management issues and works perfectly with Claude's custom connector.
app.post("/mcp", async (req: Request, res: Response) => {
  // Parse body manually to avoid stream consumption issues
  let body: any;
  try {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    body = JSON.parse(Buffer.concat(chunks).toString());
  } catch (e) {
    res.status(400).json({ jsonrpc: "2.0", error: { code: -32700, message: "Parse error" }, id: null });
    return;
  }

  const server = createMcpServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });

  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, body);
  } catch (error) {
    console.error("Error handling MCP request:", error);
    if (!res.headersSent) {
      res.status(500).json({ jsonrpc: "2.0", error: { code: -32603, message: "Internal server error" }, id: null });
    }
  }

  res.on("close", () => {
    transport.close();
    server.close();
  });
});

// GET /mcp - not supported in stateless mode
app.get("/mcp", (_req: Request, res: Response) => {
  res.status(405).json({ jsonrpc: "2.0", error: { code: -32000, message: "Method not allowed. Use POST." }, id: null });
});

// DELETE /mcp - not supported in stateless mode
app.delete("/mcp", (_req: Request, res: Response) => {
  res.status(405).json({ jsonrpc: "2.0", error: { code: -32000, message: "Method not allowed." }, id: null });
});

// Also support /sse for backwards compatibility - redirect to info
app.get("/sse", (_req, res) => {
  res.status(410).json({
    message: "This server now uses Streamable HTTP transport. Connect to POST /mcp instead.",
    endpoint: "/mcp",
    transport: "streamable-http",
  });
});

// Start the server
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Meta Ads Media MCP Server v2.0.0 running on port ${PORT}`);
  console.log(`Transport: Streamable HTTP (stateless)`);
  console.log(`MCP endpoint: POST http://0.0.0.0:${PORT}/mcp`);
  console.log(`Health check: http://0.0.0.0:${PORT}/health`);
});
