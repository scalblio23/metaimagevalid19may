# Meta Ads Media MCP Server (Remote / SSE)

A custom Model Context Protocol (MCP) server designed to be hosted remotely (e.g., on Railway) and connected to **Claude online (claude.ai)** via the Custom Integrations feature.

This server enables Claude to upload images and videos directly to Meta Ads Manager by passing public URLs. It bypasses the chunk size limitations of standard MCP integrations (like Composio) by handling the heavy lifting of the multipart form-data upload server-side.

## Features

- **URL Uploads**: Upload images and videos directly from public URLs.
- **Large Video Support**: Automatically handles chunked resumable uploads for videos up to 4GB.
- **Status Checking**: Check the processing status of uploaded videos.
- **Library Browsing**: List existing images in your Meta Ad Account.
- **Railway Ready**: Pre-configured with `railway.toml`, `nixpacks.toml`, and `Procfile` for 1-click deployment.

## Prerequisites

- A Meta Developer App with `ads_management` permissions
- A Meta System User Access Token
- Your Meta Ad Account ID
- A Railway account (or similar hosting provider)

## 1-Click Deployment to Railway

1. Push this repository to your GitHub account.
2. Log into [Railway](https://railway.app/) and click **New Project** > **Deploy from GitHub repo**.
3. Select your repository.
4. Go to the **Variables** tab in Railway and add the following environment variables:

```env
META_ACCESS_TOKEN=your_meta_access_token_here
META_AD_ACCOUNT_ID=act_XXXXXXXXX
META_APP_ID=your_app_id_here
GRAPH_API_VERSION=v25.0
AUTH_TOKEN=your_secret_token_here  # Optional: to secure your endpoint
```

5. Railway will automatically build and deploy the server.
6. Go to the **Settings** tab in Railway and click **Generate Domain** to get your public URL (e.g., `https://meta-ads-mcp-production.up.railway.app`).

## Connecting to Claude Online (claude.ai)

1. Go to your Claude settings and navigate to **Integrations** > **Custom Integrations**.
2. Click **Add Integration**.
3. Set the **URL** to your Railway domain plus `/sse` (e.g., `https://meta-ads-mcp-production.up.railway.app/sse`).
4. If you set an `AUTH_TOKEN` in Railway, add a header:
   - Key: `Authorization`
   - Value: `Bearer your_secret_token_here`
5. Save and connect.

## Available Tools

Once connected, Claude will have access to the following tools:

1. **`upload_ad_image`**: Upload an image to Meta Ads Library from a public URL. Returns the image hash.
2. **`upload_ad_video`**: Upload a video to Meta Ads Library from a public URL. Returns the video ID.
3. **`check_video_status`**: Check if an uploaded video has finished processing on Meta's servers.
4. **`list_ad_images`**: List existing images in the Meta Ad Account's image library.

## Usage Example with Claude

Once configured, you can simply tell Claude:

> "I have an ad image hosted at `https://my-vercel-site.com/ads/campaign-hero.png`. Please upload it to Meta Ads using the custom MCP, get the hash, and then use Composio to create a new ad creative with it."

Claude will use this custom MCP server to fetch the file from the URL, upload it to Meta, get the resulting `image_hash`, and then seamlessly pass that hash to Composio to finish building the ad.
