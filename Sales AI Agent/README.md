# Real Estate AI Agent

Professional customer service agent designed for property platforms in Saudi Arabia. The agent helps users find properties for sale or rent through natural language interaction.

## Features

- Multi-language support (Arabic and English)
- Conversational preference gathering (Buy vs Rent, City, Property Type, Budget)
- Real-time property search using CSV data
- Property detail retrieval and filter refinement suggestions
- Integration with Azure OpenAI and LangGraph

## Prerequisites

- Python 3.10+
- Azure OpenAI Service account and deployment

## Installation

1. Install dependencies:
   pip install -r requirements.txt

2. Configure environment variables in a .env file:
   AZURE_OPENAI_API_KEY=your_api_key
   AZURE_OPENAI_ENDPOINT=your_endpoint
   AZURE_OPENAI_API_VERSION=2024-02-01
   AZURE_OPENAI_DEPLOYMENT_NAME=your_deployment_name

## Running the Application

1. Start the FastAPI server:
   uvicorn app.main:app --reload

2. Access the agent:
   Open http://127.0.0.1:8000 in your browser to interact with the chat interface.

## API Endpoints

- POST /chat: Send messages to the agent. Requires a JSON body with 'message' and an optional 'session_id'.
- GET /: Serves the web-based chat interface.
