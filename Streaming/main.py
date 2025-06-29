from processor import DebeziumKafkaProcessor
import sys
import traceback

def main():
    try:
        processor = DebeziumKafkaProcessor()
        processor.run()
    except KeyboardInterrupt:
        print("Streaming interrupted by user.")
    except Exception as e:
        print("An error occurred while running the DebeziumKafkaProcessor:")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
