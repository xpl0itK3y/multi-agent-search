from pydantic import BaseModel, Field

class OptimizeRequest(BaseModel):
    prompt: str = Field(..., description="The raw prompt to be optimized", min_length=1)

class OptimizeResponse(BaseModel):
    optimized_prompt: str = Field(..., description="The improved and structured prompt")
    status: str = "success"
