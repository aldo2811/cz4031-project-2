import os

for filename in os.listdir("../data"):
    name = os.path.splitext(filename)[0]
    print(f"cleaning {filename}")
    if os.path.isdir(f"../data/{filename}"):
        continue
    with open(f"../data/{filename}", "r") as f:
        with open(f"../data/cleaned_data/{name}.csv", "w") as f2:
            for line in f.readlines():
                f2.write(line.replace("|\n", "\n"))
