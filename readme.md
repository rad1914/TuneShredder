# TuneShredder

**TuneShredder** is a high-performance audio fingerprinting and deduplication engine. It leverages machine learning to extract acoustic features from audio files, generate unique embeddings, and identify duplicate or highly similar tracks across large libraries—regardless of metadata or file naming conventions.

---

## 🚀 What It Does

The system operates through a three-stage pipeline to manage and clean audio collections:

1.  **Acoustic Indexing**: Scans directories and uses **TensorFlow.js** and **Meyda** to extract MFCCs (Mel-frequency cepstral coefficients) and spectral features (Centroid, Flatness, Rolloff). These are passed through a dense neural network to generate fixed-length embeddings.
2.  **Similarity Analysis**: Compares audio embeddings using a **sliding window dot-product** algorithm. It identifies matches based on a configurable similarity threshold ($>0.996$ by default), effectively finding remixes, duplicates, or different encodes of the same track.
3.  **Smart Deduplication**: Utilizes a **Union-Find** data structure to group related audio files. It automatically selects a "keeper" (based on file size/mtime) and moves "losers" (duplicates) to a dedicated directory for review.
4.  **Source Maintenance**: Includes a utility script to clean source code, manage path headers, and strip unnecessary annotations.

---

## 🛠 Tech Stack

| Category | Technology |
| :--- | :--- |
| **Runtime** | Node.js (v18+) |
| **Machine Learning** | TensorFlow.js (`@tensorflow/tfjs-node`) |
| **Audio Processing** | FFmpeg (`ffmpeg-static`), Meyda |
| **Data Structures** | Union-Find (Disjoint Set Union) |
| **File I/O** | Native Node.js `fs/promises`, `path` |

---

## 📦 How To Install & Run

### Prerequisites
*   Node.js installed on your machine.
*   Sufficient disk space for the generated `.emb` database files.

### Installation
```bash
# Clone the repository
git clone https://github.com/rad1914/TuneShredder.git
cd TuneShredder

# Install dependencies
npm install @tensorflow/tfjs-node ffmpeg-static meyda
```

### Execution
The project uses a central dispatcher (`main.js`) to trigger specific modules:

1.  **Index your library**:
    ```bash
    node main.js index /path/to/your/music/folder
    ```
2.  **Find matches**:
    ```bash
    node main.js query
    ```
3.  **Process duplicates**:
    ```bash
    node main.js ab
    ```

---

## ☁️ Deployment Target

*   **Local Machine / Workstation**: Ideal for personal music collection management via CLI.
*   **Docker**: Can be containerized for headless server environments or NAS (Network Attached Storage) automation.
*   **AWS / Cloud**: The indexing logic can be ported to AWS Lambda or EC2 instances for large-scale cloud-based audio processing.

---

## ⚙️ Configuration & APIs

The application uses several internal constants that can be modified within the source files to tune performance:

### Core Configuration Variables
| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `SAMPLE_RATE` | `44100` | Target audio sample rate for processing. |
| `EMB_DIM` | `96` | Dimensions of the generated audio embedding vector. |
| `SCORE_THRESHOLD` | `0.996` | Similarity score (0.0 to 1.0) required to flag a match. |
| `PARALLEL` | `8` | Number of simultaneous audio files being indexed. |
| `DB_DIR` | `./db` | Directory where embedding files and song metadata are stored. |
| `DUPE_DIR` | `./dupe` | Directory where identified duplicate files are moved. |

### Neural Network Structure
The system automatically initializes a sequential model if one is not found:
*   **Input**: 32-dimension feature vector.
*   **Hidden Layers**: Dense (256, ReLU) $\rightarrow$ Dense (128, ReLU).
*   **Output**: Dense (96 units, Embedding Layer).

---

## 📄 License
This project is licensed under the **MIT License**.
~ Made with <3 by @RADWrld
