import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
import json
import time

# Specify the model and tokenizer names or paths
model_name = "/home/paperspace/rajdeep/DeepSpeedExamples/applications/DeepSpeed-Chat/training/step1_supervised_finetuning/output_opt350_success/"
tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=True)
model = AutoModelForCausalLM.from_pretrained(model_name, local_files_only=True)

def generate_tcp_packet(prompt):
    """
    Generate the next TCP packet prediction based on the given prompt.
    """
    input_ids = tokenizer.encode(prompt, return_tensors="pt")
    with torch.no_grad():
        start_time = time.time()
        output = model.generate(input_ids, max_length=700, num_return_sequences=1)
        end_time = time.time()

    generated_text = tokenizer.decode(output[0], skip_special_tokens=True)
    time_taken = end_time - start_time
    return generated_text, time_taken

input_file = "/home/paperspace/rajdeep/DeepSpeedExamples/applications/DeepSpeed-Chat/training/step1_supervised_finetuning/accuracy_testing/eval.json"
with open(input_file, 'r') as file:
    data = json.load(file)

#data = data[:500]  # Limit the data to the first 500 items

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
print("Number of correct predictions:{correct_predictions}")
print(f"Accuracy: {accuracy}%")
