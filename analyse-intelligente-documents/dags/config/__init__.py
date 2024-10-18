# @Author: Zoumana Keita

class Config: 
    # OpenAI
    OPENAI_API_VERSION = ""
    OPENAI_END_POINT = ""
    GPT_DEPLOYMENT_NAME = "gpt-4-turbo"
    GPT_PROMPT_PATH = "./prompts/prompt-analyse-documents-scientifiques.txt"
    GPT_KEY = ""
    GPT_TEMPERATURE = 0.35
    GPT_MAX_TOKEN = 1000

    # Azure Doc Intelligence
    OCR_API_VERSION = ""
    OCR_MODEL_ID = ""
    OCR_KEY = ""
    OCR_ENDPOINT = ""

config = Config()