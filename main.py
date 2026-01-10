import os


def main():
    # This block executes when the script is run directly
    print("Hello, World!")

    # Optional: Display the script's filename and full path
    print(f"Script name: {os.path.basename(__file__)}")
    print(f"Script path: {os.path.abspath(__file__)}")


if __name__ == "__main__":
    # This is the entry point of the script when executed from the command line
    main()
