import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
import json
import time

# Specify the device (GPU if available, otherwise CPU)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Specify the model and tokenizer names or paths
model_name = "llama2_13b_50k"
tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=True)
model = AutoModelForCausalLM.from_pretrained(model_name, local_files_only=True)
model.to(device)  # Move model to GPU if available

def generate_tcp_packet(prompt):
    """
    Generate the next TCP packet prediction based on the given prompt.
    """
    input_ids = tokenizer.encode(prompt, return_tensors="pt").to(device)  # Move input tensor to GPU if available
    with torch.no_grad():
        start_time = time.time()
        output = model.generate(input_ids, max_length=700, num_return_sequences=1)
        end_time = time.time()

    generated_text = tokenizer.decode(output[0], skip_special_tokens=True)
    time_taken = end_time - start_time
    return generated_text, time_taken

input_file = "eval.json"
with open(input_file, 'r') as file:
    data = json.load(file)

data = data[:10]
output_file = 'output_predictions_test_your_model_name_with_num.json'
output_data = []
correct_predictions = 0

for item in data:
    prompt = item['prompt']
    correct_answer = item['chosen']  # Assuming you want to compare against the 'chosen' answer

    next_tcp_packet, time_taken = generate_tcp_packet(prompt)

    # Compare prediction with the correct answer
    if next_tcp_packet.strip() == correct_answer.strip():
        correct_predictions += 1

    # Store results
    output_data.append({
        'Prompt': prompt,
        'Predicted_TCP_Packet': next_tcp_packet,
        'Correct_Answer': correct_answer,
        'Time_Taken': time_taken
    })

    print("Processed one item. Time Taken:", time_taken, "seconds")

# Calculate the accuracy
accuracy = (correct_predictions / len(data)) * 100

# After processing all items
with open(output_file, 'w') as outfile:
    json.dump(output_data, outfile, indent=4)

print("Predictions saved to:", output_file)
print(f"Number of correct predictions: {correct_predictions}")
print(f"Accuracy: {accuracy}%")
