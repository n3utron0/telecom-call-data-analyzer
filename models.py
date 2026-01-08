#models.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

# ------------------------------------------------------------
# Response Model for Frontend
# ------------------------------------------------------------
class ExtractedCallData(BaseModel):
    customer_id: str = Field(..., description="Randomly generated customer ID")
    phone_number: Optional[str] = Field(None, description="10-digit phone number if detected")
    complaint_type: Optional[str] = Field(
        None, description="Recharge Issue, Payment Issue, Network Issue, or Others"
    )
    customer_sentiment: Optional[str] = Field(
        None, description="positive, negative, or neutral"
    )
    resolved: bool = Field(..., description="Whether the issue was resolved or not")
    transcript: Optional[str] = Field(None, description="Transcript of the conversation")
    processing_time_sec: Optional[float] = Field(
        None, description="Time taken to process this file in seconds"
    )

# ------------------------------------------------------------
# Prompt Template for Gemini
# ------------------------------------------------------------
GEMINI_PROMPT = """
You are an AI assistant analyzing a telecom customer support call recording.

Your task:
1. Generate a complete transcript of the conversation.
2. Extract the following structured information:

**Required Fields:**
- **phone_number**: Extract the customer's 10-digit phone number from the conversation (string). If not mentioned, return "Not Found".
- **complaint_type**: Classify the complaint into ONE of these categories:
  - "Recharge Issue"
  - "Payment Issue"
  - "Network Issue"
  - "Others" (if it doesn't fit the above or unclear)
- **customer_sentiment**: Analyze the customer's overall sentiment and classify as:
  - "Positive"
  - "Negative"
  - "Neutral"
- **resolved**: Determine if the issue was resolved by the end of the call:
  - true (if resolved or customer satisfied)
  - false (if unresolved or customer still has concerns)

**Output Format:**
Return ONLY a valid JSON object with this exact structure (no extra text):

{
  "transcript": "full conversation transcript here",
  "phone_number": "1234567890",
  "complaint_type": "Network Issue",
  "customer_sentiment": "Negative",
  "resolved": false
}

**Important:**
- Be precise with the phone number (must be exactly 10 digits).
- Base your classification on the actual content of the call.
- If information is unclear, use your best judgment.
"""

class UploadResponse(BaseModel):
    file_id: str
    status: str

class ProcessStatus(BaseModel):
    queue_size: int
    in_flight: int
    total_processed: int
    total_success: int
    total_failed: int
    last_batch: Optional[Dict[str, Any]] = None
