# Hadoop MapReduce — Word Count

A Java MapReduce program that counts word frequencies across input text files using Apache Hadoop.

---

## How It Works

```
Input Files
    │
    ▼
[Mapper]   → tokenizes each line → emits (word, 1) per word
    │
[Combiner] → local mini-reduce per node (reduces network traffic)
    │
[Reducer]  → sums all counts per word → emits (word, totalCount)
    │
    ▼
output/part-r-00000
```

---

## Prerequisites

| Tool | Purpose |
|---|---|
| Java JDK 8+ | Compile & run |
| Apache Hadoop 3.x | MapReduce framework |
| WSL / Linux | Terminal environment |

---

## Project Structure

```
MapReduceWorkspace/
├── WordCount.java          # Source code
├── wordcount.jar           # Built JAR (generated)
├── wordcount_classes/      # Compiled classes (generated)
├── input/                  # Put your .txt files here
│   ├── file1.txt
│   └── file2.txt
└── output/                 # Created by Hadoop (do NOT pre-create)
    └── part-r-00000
```

---

## Source Code — `WordCount.java`

```java
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // ─── MAPPER ───────────────────────────────────────────────────────────────
    // Reads one line at a time, splits into words, emits (word, 1) for each word
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // ─── REDUCER ──────────────────────────────────────────────────────────────
    // Receives (word, [1, 1, 1, ...]) and sums up all counts into a final total
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // ─── DRIVER ───────────────────────────────────────────────────────────────
    // Configures and submits the Hadoop job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // local pre-reduce to cut network I/O
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

---

## Steps to Run

**1. Set up folders**
```bash
cd ~
mkdir -p MapReduceWorkspace/input
cd MapReduceWorkspace
```

**2. Add input files**
```bash
echo "coding in wsl is fast" > input/file1.txt
echo "wsl is powerful"       > input/file2.txt
```

**3. Compile**
```bash
mkdir wordcount_classes
javac -classpath $(hadoop classpath) -d wordcount_classes WordCount.java
```

**4. Package JAR**
```bash
jar -cvf wordcount.jar -C wordcount_classes/ .
```

**5. Run**
> ⚠️ The `output/` folder must not already exist. Delete it between runs: `rm -rf output`

```bash
hadoop jar wordcount.jar WordCount input output
```

**6. View results**
```bash
cat output/part-r-00000
```

---

## Sample Output

```
coding      1
fast        1
in          1
is          2
powerful    1
wsl         2
```

---

## Code Summary

| Class | Role |
|---|---|
| `TokenizerMapper` | Splits each line into words; emits `(word, 1)` for every token |
| `IntSumReducer` | Sums all values for a given word; emits `(word, total)` |
| `main()` (Driver) | Configures the job, registers classes, sets I/O paths, submits |

> **Combiner:** `IntSumReducer` is also set as the Combiner — it runs a local reduce on each mapper node before the shuffle phase, significantly cutting down the data sent over the network.

---

## Common Errors

| Error | Fix |
|---|---|
| `output` already exists | `rm -rf output` before re-running |
| `ClassNotFoundException` | Re-package the JAR with the `jar -cvf` command |
| `javac: command not found` | `sudo apt install default-jdk` |
| `hadoop: command not found` | Add Hadoop `bin/` to your `PATH` in `~/.bashrc` |
