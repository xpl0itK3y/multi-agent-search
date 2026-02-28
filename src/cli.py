import sys
from src.providers.deepseek import DeepSeekProvider
from src.agents.optimizer import PromptOptimizerAgent
from src.config import settings

def safe_input(prompt: str) -> str:
    sys.stdout.write(prompt)
    sys.stdout.flush()
    try:
        line = sys.stdin.buffer.readline()
        if not line:
            return "exit"
        return line.decode('utf-8', errors='replace').strip()
    except Exception:
        return "exit"

def main():
    print(f"--- {settings.app_name} (CLI) ---")
    print("Type 'exit' or 'quit' to exit.\n")

    try:
        llm = DeepSeekProvider(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        agent = PromptOptimizerAgent(llm)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error during initialization: {e}")
        sys.exit(1)

    while True:
        try:
            user_input = safe_input("You: ")
            
            if not user_input:
                continue
                
            if user_input.lower() in ['exit', 'quit']:
                print("Exiting...")
                break

            print("\nOptimizing...")
            optimized_prompt = agent.run(user_input)
            
            print(f"\n--- Optimized Prompt ---\n{optimized_prompt}\n")
            print("-" * 30 + "\n")

        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"An error occurred: {e}\n")

if __name__ == "__main__":
    main()
