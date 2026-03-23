import os
import glob
import tensorflow as tf

_DATA        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "Youtube")
TFRECORD_DIR = os.path.join(_DATA, "yt8m", "video_level")
OUTPUT_TXT   = os.path.join(_DATA, "video_ids_all_train.txt")

tfrecord_files = sorted(
    glob.glob(os.path.join(TFRECORD_DIR, "*.tfrecord")) +
    glob.glob(os.path.join(TFRECORD_DIR, "*.tfrecord.gz"))
)
total_ids = 0
bad_files = []

with open(OUTPUT_TXT, "w", encoding="utf-8") as f_out:
    for path in tfrecord_files:
        fname = os.path.basename(path)
        print(f"\nProcessing: {fname}")

        try:
            ds = tf.data.TFRecordDataset(path)  # one file at a time
            count_file = 0

            for raw in ds:
                ex = tf.train.Example()
                ex.ParseFromString(raw.numpy())
                vid = ex.features.feature["id"].bytes_list.value[0].decode("utf-8")
                f_out.write(vid + "\n")
                count_file += 1

            total_ids += count_file
            print(f"  OK: {count_file} ids")

        except tf.errors.DataLossError as e:
            print(f"DATA_LOSS in {fname}: {e}")
            bad_files.append(path)
            continue

print(f"\nDone. Extracted {total_ids} ids -> {OUTPUT_TXT}")

if bad_files:
    print("\nBad / corrupted TFRecord files:")
    for p in bad_files:
        print(" -", p)
