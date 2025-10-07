import os
import json
from openai import OpenAI
from dotenv import load_dotenv  

# --- Load environment variables from .env file ---
load_dotenv() 

# --- Configuration ---
# It's best practice to use environment variables for API keys
API_KEY = os.getenv("AIMLAPI_KEY") 
MODEL = "gpt-3.5-turbo"

# --- Initialize the API Client ---
# This setup points the standard OpenAI library to the AIMLAPI endpoint.
client = OpenAI(
    base_url="https://api.aimlapi.com/v1",
    api_key=API_KEY,
)

# --- Style Management ---
# In a real application, this would be stored in a database.
# For this script, we'll use a simple dictionary.
CUSTOM_STYLES = {}

PREDEFINED_STYLES = [
    "Respectful & Formal",
    "Persuasive & Confident",
    "Conversational & Casual",
    "Storytelling & Engaging",
    "Academic & Scholarly",
    "Explanatory & Simple",
    "Humorous & Witty",
]

def add_custom_style(name: str, example_text: str):
    """
    Adds a new custom style defined by an example text.
    The user gives it a name, and we store the example.
    """
    if not name or not example_text:
        print("Error: Both style name and example text are required.")
        return
    CUSTOM_STYLES[name] = example_text
    print(f"✅ Custom style '{name}' added successfully!")


def get_ai_score(text_to_analyze: str) -> dict:
    """
    Analyzes text and returns an AI-generated score and reason using gpt-3.5-turbo.
    """
    prompt = f"""
    You are an AI writing detector. Your task is to analyze the following text and determine the probability that it was written by an AI. Analyze it based on factors like perplexity, burstiness, sentence structure uniformity, and lack of a distinct personal voice.

    Provide your response in a strict JSON format with two keys: "score" (a number between 0 and 100) and "reason" (a brief, one-sentence explanation for your score).

    Text to analyze:
    \"\"\"
    {text_to_analyze}
    \"\"\"
    """
    
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2, # Low temperature for consistent JSON
            response_format={"type": "json_object"} # Enforces JSON output
        )
        
        result = json.loads(response.choices[0].message.content)
        return result

    except Exception as e:
        print(f"An error occurred during AI detection: {e}")
        return {"score": -1, "reason": "Error analyzing text."}

def humanize_text(text_to_humanize: str, style_name: str) -> str:
    """
    Rewrites text in a specified style. 
    It can use a predefined style name or a custom style defined by an example.
    """
    # Check if the requested style is a custom one
    if style_name in CUSTOM_STYLES:
        example = CUSTOM_STYLES[style_name]
        prompt = f"""
        You are a writing style chameleon. Your task is to rewrite the "Original Text" to perfectly match the tone, voice, and style of the "Style Example" provided.

        --- Style Example ---
        {example}
        ---

        --- Original Text to Rewrite ---
        {text_to_humanize}
        ---

        Rewrite the "Original Text" below, adopting the style from the example. Do not add any commentary.
        """
    # Otherwise, use the predefined style logic
    elif style_name in PREDEFINED_STYLES:
        prompt = f"""
        You are a text humanizer and expert writer. Rewrite the following text to sound natural and human, adopting the specified style. Make it pass as if written by a person with that specific tone. Do not add any commentary.

        Style to adopt: {style_name}

        Original Text:
        \"\"\"
        {text_to_humanize}
        \"\"\"

        Rewritten Humanized Text:
        """
    else:
        return f"Error: Style '{style_name}' not found. Please choose from predefined styles or add a new custom one."

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.75, # Slightly higher for more creative and human-like variation
            top_p=0.9,
            max_tokens=2048
        )
        
        humanized_text = response.choices[0].message.content.strip()
        return humanized_text

    except Exception as e:
        print(f"An error occurred during humanization: {e}")
        return "Error: Could not humanize the text."


# ==============================================================================
# ---                        EXAMPLE USAGE                                   ---
# ==============================================================================
if __name__ == "__main__":
    # The robotic, AI-sounding text we want to fix
    original_text = "The utilization of advanced artificial intelligence paradigms facilitates the creation of highly efficient and automated content generation systems. This results in a marked improvement in productivity across various sectors."

    print("--- 1. Analyzing the Original Text ---")
    analysis_result = get_ai_score(original_text)
    print(f"🚨 AI Score: {analysis_result.get('score')}%")
    print(f"Reason: {analysis_result.get('reason')}\n")

    # --- 2. Humanizing with a PREDEFINED Style ---
    print("--- 2. Humanizing with a Predefined Style ('Conversational & Casual') ---")
    humanized_v1 = humanize_text(original_text, "Conversational & Casual")
    print(humanized_v1)
    print("-" * 50)

    # --- 3. Creating a NEW CUSTOM Style by Example ---
    print("\n--- 3. Defining a New Custom Style by Example ---")
    my_style_name = "Dave's Snarky Tech Blog"
    my_style_example = """
    Alright, let's be real. Half the 'AI' tools out there are just a bunch of if-statements in a trench coat. But every now and then, something genuinely cool pops up that actually saves you time instead of just making you want to throw your laptop out the window. That's the stuff I live for.
    """
    add_custom_style(name=my_style_name, example_text=my_style_example)
    print(f"Available custom styles: {list(CUSTOM_STYLES.keys())}")
    print("-" * 50)

    # --- 4. Humanizing with our NEW CUSTOM Style ---
    print(f"\n--- 4. Humanizing with our Custom Style ('{my_style_name}') ---")
    humanized_v2 = humanize_text(original_text, my_style_name)
    print(humanized_v2)
    print("-" * 50)