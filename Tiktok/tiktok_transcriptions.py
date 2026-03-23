import json
import math
import re
import sys
from collections import defaultdict
import requests


def group_by(lst, key_extractor):
    d = defaultdict(list)
    for item in lst:
        d[key_extractor(item)].append(item)
    return d


def get_transcripts_for_tiktok_video(video_id, transcripts_dir):
    video_url = f"https://www.tiktok.com/@unknown/video/{video_id}"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    headers = {
        "user_agent": user_agent
    }
    response = requests.get(video_url, headers=headers)
    html_content = response.text

    # Extracting the JSON object from the HTML file
    json_match = re.search(r'(?<="__DEFAULT_SCOPE__":)[^<]*', html_content)
    if not json_match:
        print("JSON data not found in the HTML content.")
        sys.exit(1)

    json_data = json.loads(json_match.group(0).strip()[:-1])  # manually removing last character
    transcripts_infos = json_data["webapp.video-detail"]["itemInfo"]["itemStruct"]["video"]["subtitleInfos"]

    language_code_priority = [
        "eng-US",
        "fra-FR",
        "deu-DE",
        "spa-ES",
        "ita-IT",
        "rus-RU",
        "kor-KR",
        "jpn-JP",
        "ara-SA",
        "ind-ID",
        "por-PT",
        "cmn-Hans-CN",
        "vie-VN",
    ]
    subtitle_infos_by_format = group_by(transcripts_infos, lambda info: info["Format"])
    for subtitle_format, infos_list in subtitle_infos_by_format.items():
        # prioritise by language and collect the transcripts for the language with the highest priority
        sorted_transcripts_infos_list = sorted(transcripts_infos,
                                               key=lambda info: language_code_priority.index(
                                                   info["LanguageCodeName"]) if
                                               info["LanguageCodeName"] in language_code_priority else math.inf)
        transcripts_info = sorted_transcripts_infos_list[0]
        url = transcripts_info["Url"]
        language = transcripts_info["LanguageCodeName"]
        source = transcripts_info["Source"]

        match subtitle_format:
            case "webvtt":
                suffix = "vtt"
            case "creator_caption":
                suffix = "json"
            case _:
                suffix = None

        # Download the file using requests and save with appropriate name
        filename = f"{video_id}_{subtitle_format}_{language}_{source}"
        if suffix:
            filename += f".{suffix}"
        file_response = requests.get(url, headers=headers)
        if file_response.ok:
            video_dir = transcripts_dir / video_id
            video_dir.mkdir(exist_ok=True)
            with open(video_dir / filename, "wb+") as f:
                f.write(file_response.content)
        else:
            print(f"Failed to download transcripts for video {video_id} for language {language}")
