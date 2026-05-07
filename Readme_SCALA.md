# 🚀 Apache Spark with Scala — Getting Started Guide

A step-by-step guide to running Scala programs (Bubble Sort & Word Count) using Apache Spark Shell on WSL (Windows Subsystem for Linux).

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Phase 1 — Installation](#phase-1--installation)
4. [Phase 2 — Launching the Spark Shell](#phase-2--launching-the-spark-shell)
5. [Phase 3 — Writing & Running Your Programs](#phase-3--writing--running-your-programs)
   - [Program 1: Bubble Sort](#program-1-bubble-sort)
   - [Program 2: Word Count](#program-2-word-count)
6. [How to Exit](#how-to-exit)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before you begin, make sure you have the following installed on your system:

| Tool | Purpose | Check Command |
|------|---------|---------------|
| WSL (Ubuntu) | Linux environment on Windows | `wsl --version` |
| Java (JDK 8 or 11) | Required to run Spark | `java -version` |
| Apache Spark 3.5.1 | The processing engine | (installed below) |
| `nano` | Terminal text editor for writing `.scala` files | `nano --version` |

### Installing Java (if not already installed)

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y

# Verify installation
java -version
```

### Downloading Apache Spark

```bash
# Download Spark 3.5.1
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz

# Extract the archive
tar -xvzf spark-3.5.1-bin-hadoop3.tgz

# Move into the Spark directory
cd spark-3.5.1-bin-hadoop3
```

---

## Project Structure

Once everything is set up, your working directory should look like this:

```
spark-3.5.1-bin-hadoop3/
│
├── bin/
│   └── spark-shell          ← The interactive Scala REPL
│
├── bubble_sort.scala         ← Program 1 (you will create this)
├── word_count.scala          ← Program 2 (you will create this)
│
└── /mnt/c/Users/RUCHIR/Downloads/
    └── file.txt              ← Input file for Word Count (on your Windows drive)
```

---

## Phase 1 — Installation

> ✅ If you completed the Prerequisites section above, you are ready. Skip to Phase 2.

Make sure you are inside the Spark directory for all subsequent commands:

```bash
cd spark-3.5.1-bin-hadoop3
```

---

## Phase 2 — Launching the Spark Shell

The **Spark Shell** is an interactive REPL (Read-Eval-Print Loop) that runs Scala code with Spark already configured for you. Key variables like `sc` (SparkContext) and `spark` (SparkSession) are automatically available.

Launch the shell from inside the `spark-3.5.1-bin-hadoop3` directory:

```bash
./bin/spark-shell
```

You will see some startup log output. Once it is ready, you will see the prompt:

```
scala>
```

You are now inside the Spark Shell and can begin loading your programs.

---

## Phase 3 — Writing & Running Your Programs

### The Workflow (for every program)

Instead of pasting code directly into the shell, we use a cleaner 3-step workflow:

```
Step 1 → Write your code in a .scala file using nano
Step 2 → Open spark-shell
Step 3 → Load and run the file with :load filename.scala
```

This keeps your code saved, editable, and reusable.

---

### Program 1: Bubble Sort

#### Step 1 — Create the file

Open a **new terminal tab** (keep spark-shell for later) and navigate to your Spark directory:

```bash
cd spark-3.5.1-bin-hadoop3
nano bubble_sort.scala
```

The `nano` editor will open. Type or paste the following code:

```scala
// ── Bubble Sort in Scala ──────────────────────────────────────────────────

// 1. Define the sorting function
def bubbleSort(arr: Array[Int]): Unit = {
  val n = arr.length
  for (i <- 0 until n - 1) {
    for (j <- 0 until n - i - 1) {
      if (arr(j) > arr(j + 1)) {
        val temp = arr(j)
        arr(j) = arr(j + 1)
        arr(j + 1) = temp
      }
    }
  }
}

// 2. Initialize data and track execution time
val nums = Array(64, 34, 25, 12, 22, 11, 90)
val startTime = System.nanoTime()

bubbleSort(nums)

val endTime = System.nanoTime()
val durationMs = (endTime - startTime) / 1e6

// 3. Print results
println(s"Sorted array: ${nums.mkString(", ")}")
println(f"Execution time: $durationMs%.4f ms")
```

**Save and exit nano:**
- Press `Ctrl + O` → then `Enter` to save
- Press `Ctrl + X` to exit

#### Step 2 — Launch the Spark Shell

```bash
./bin/spark-shell
```

#### Step 3 — Load and run the file

At the `scala>` prompt, type:

```scala
:load bubble_sort.scala
```

#### Expected Output

```
Sorted array: 11, 12, 22, 25, 34, 64, 90
Execution time: 0.0312 ms
```

---

### Program 2: Word Count

This program uses Spark's distributed RDD (Resilient Distributed Dataset) API to read a text file and count how many times each word appears.

#### Step 0 — Prepare your input file

> ⚠️ **Important:** You must create the input file on your Windows filesystem before running this program.

Open **File Explorer** on Windows, navigate to `C:\Users\RUCHIR\Downloads\`, and create a new file called `file.txt`. Add a few sentences of text, for example:

```
hello world hello spark
spark is fast spark is powerful
hello from scala
```

Save the file. WSL will access it automatically at the path `/mnt/c/Users/RUCHIR/Downloads/file.txt`.

#### Step 1 — Create the file

In your WSL terminal (inside the Spark directory):

```bash
nano word_count.scala
```

Type or paste the following code:

```scala
// ── Word Count using Apache Spark RDD ────────────────────────────────────

// 1. Define the path to your input text file
//    WSL maps your Windows C:\ drive to /mnt/c/
val path = "/mnt/c/Users/RUCHIR/Downloads/file.txt"

// 2. Load the file into an RDD (one entry per line)
val rdd = sc.textFile(path)

// 3. Run the Map-Reduce pipeline:
//    - flatMap  : split each line into individual words
//    - map      : tag each word with a count of 1
//    - reduceByKey : sum all counts for matching words
val counts = rdd
  .flatMap(line => line.split(" "))
  .map(word => (word, 1))
  .reduceByKey(_ + _)

// 4. Collect results from all partitions and print
println("\n── Word Counts ──────────────────")
counts.collect().foreach { case (word, count) =>
  println(s"  $word → $count")
}
println("─────────────────────────────────\n")
```

**Save and exit nano:**
- Press `Ctrl + O` → then `Enter`
- Press `Ctrl + X`

#### Step 2 — Launch the Spark Shell (if not already open)

```bash
./bin/spark-shell
```

#### Step 3 — Load and run the file

```scala
:load word_count.scala
```

#### Expected Output

```
── Word Counts ──────────────────
  hello → 3
  world → 1
  spark → 3
  is → 2
  fast → 1
  powerful → 1
  from → 1
  scala → 1
─────────────────────────────────
```

> 📝 **Note:** The order of words in the output may vary, as Spark processes data in parallel partitions. The counts will always be correct.

---

## How to Exit

When you are done, exit the Spark Shell using either of these methods:

```scala
:quit
```

or press `Ctrl + C`.

---

## Troubleshooting

### ❌ `JAVA_HOME` not set or Java not found

```bash
# Find your Java installation path
which java
readlink -f $(which java)

# Export the path (replace with your actual path)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

To make this permanent, add the export line to your `~/.bashrc`:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

---

### ❌ `No such file or directory` for `file.txt`

- Make sure the file exists at `C:\Users\RUCHIR\Downloads\file.txt` on Windows.
- Verify WSL can see it:
  ```bash
  ls /mnt/c/Users/RUCHIR/Downloads/file.txt
  ```
- Check for typos in the path inside `word_count.scala`.

---

### ❌ `:load` gives a compilation error

- Open the `.scala` file again with `nano filename.scala` and double-check for missing brackets or typos.
- Make sure you are running `:load` from the same directory where the `.scala` file is saved.
- You can confirm your current directory inside spark-shell with:
  ```scala
  import sys.process._
  "pwd".!
  ```

---

### ❌ Spark Shell takes too long to start or shows too many logs

To reduce log verbosity, edit the log4j configuration:

```bash
cp conf/log4j2.properties.template conf/log4j2.properties
nano conf/log4j2.properties
```

Find the line with `rootLogger.level` and change it to:

```
rootLogger.level = WARN
```

Save and restart the shell.

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Open/create a Scala file | `nano filename.scala` |
| Save in nano | `Ctrl + O`, then `Enter` |
| Exit nano | `Ctrl + X` |
| Start Spark Shell | `./bin/spark-shell` |
| Load a Scala file | `:load filename.scala` |
| Exit Spark Shell | `:quit` or `Ctrl + C` |
| Check current directory | `pwd` |
| List files | `ls` |

---

*Built with Apache Spark 3.5.1 · Scala · WSL (Ubuntu)*
