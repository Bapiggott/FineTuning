import json
import os
import re


def extract_context_and_packet(packet_text):
    prompt_match = re.search(r"task: (.*?) Context", packet_text, re.DOTALL)
    context_match = re.search(r"Context: (.*?) Question", packet_text, re.DOTALL)
    question_match = re.search(r"Question: (.*?)}", packet_text, re.DOTALL)
    packet_match = re.search(r"Question:.*?\{.*?\}\s*(\{.*?\})", packet_text, re.DOTALL)

    if context_match and packet_match:
        #print('hi')
        prompt_text = prompt_match.group(1).strip()
        context_text = context_match.group(1).strip()
        question_text = question_match.group(1).strip()
        packet_text = packet_match.group(1).strip()

        # Replace placeholder texts with JSON null
        replacements = {"None": "null", "none": "null", "'": '"', '\"': '"'}
        for old, new in replacements.items():
            prompt_text = prompt_text.replace(old, new)
            context_text = context_text.replace(old, new)
            question_text = question_text.replace(old, new)
            packet_text = packet_text.replace(old, new)
        # print('hi')
        try:
            #print(context_text)
            #print(packet_text)
            packet_dict = json.loads(packet_text) #json.loads(packet_text + '}')
            question_dict = json.loads(question_text + '}')
            context_dict = json.loads(context_text)

            # Transform and update packet_dict as necessary
            return prompt_text, json.dumps(context_dict), json.dumps(question_dict), json.dumps(packet_dict)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    return None, None, None, None


def extract_correct_answer(answer):
    answer_match = re.search(r"(.*)", answer, re.DOTALL)
    answer_text = answer_match.group(1).strip()
    replacements = {"None": "null", "none": "null", "'": '"'}
    for old, new in replacements.items():
        answer_text = answer_text.replace(old, new)
    answer_dict = json.loads(answer_text)
    return json.dumps(answer_dict)


json_files = [f for f in os.listdir('.') if f.endswith('.json')]
print(json_files)
for input_file_path in json_files:
    output_file_path = input_file_path.replace(".json", "_final_version.json")

    with open(input_file_path, 'r') as input_file:
        data = json.load(input_file)

    new_data = []
    for entry in data:
        if 'Predicted_TCP_Packet' in entry:
            predicted_tcp_packet = entry['Predicted_TCP_Packet']
            correctAnswer = entry['Correct_Answer']
            prompt, context, question, packet = extract_context_and_packet(predicted_tcp_packet)
            correct_answer = extract_correct_answer(correctAnswer)
            if context and packet:
                entry['Prompt'] = prompt
                entry['Context'] = json.loads(context)
                entry['Question'] = json.loads(question)
                entry['Correct_Answer'] = json.loads(correct_answer)
                entry['Predicted_TCP_Packet'] = json.loads(packet)  # Assuming packet is already a JSON string
                #print(packet)
                new_data.append(entry)

    if new_data:  # Only write if there's something to write
        with open(output_file_path, 'w') as output_file:
            json.dump(new_data, output_file, indent=4)
        print(f"File {input_file_path} processed. Output saved as {output_file_path}")
