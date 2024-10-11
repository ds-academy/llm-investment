import json


def process_json_block(block):
    try:
        data = json.loads(block)
        return json.dumps(data, ensure_ascii=False)
    except json.JSONDecodeError:
        print(f"경고: JSON 파싱 오류가 발생한 블록을 건너뜁니다:\n{block}")
        return None


def convert_file(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as infile, open(
        output_file, "w", encoding="utf-8"
    ) as outfile:
        current_block = ""
        in_block = False
        for line in infile:
            stripped_line = line.strip()
            if stripped_line == "{":
                in_block = True
                current_block = "{"
            elif stripped_line == "}" and in_block:
                current_block += "}"
                processed_block = process_json_block(current_block)
                if processed_block:
                    outfile.write(processed_block + "\n")
                in_block = False
                current_block = ""
            elif in_block:
                current_block += line


def main():
    input_file = "../../../assets/fine_tune_data.txt"
    output_file = "../../../assets/fine_tune_data.jsonl"

    convert_file(input_file, output_file)
    print(f"변환이 완료되었습니다. 결과가 {output_file}에 저장되었습니다.")


if __name__ == "__main__":
    main()
