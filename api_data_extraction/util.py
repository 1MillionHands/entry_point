

class ApiUtil:
    source_url_dict = {
        "twitter_1": "https://access.scooper.co.il/app/project/079b31af-d87b-4dbe-9689-9cb5752b10aa/cached/export_Scooper8_xoAfoLRB.json",
        "instagram_1": "https://access.scooper.co.il/app/project/079b31af-d87b-4dbe-9689-9cb5752b10aa/cached/export_Scooper8_b4NgvSLV.json",
        "reddit_1": "https://access.scooper.co.il/app/project/8408c995-d612-41ed-84ee-69d3c0fa117e/cached/export_Scooper5_qp06Q9tP.json",
        "youtube_1": "https://access.scooper.co.il/app/project/8408c995-d612-41ed-84ee-69d3c0fa117e/cached/export_Scooper5_4nbh3fBv.json",
        "tiktok_1": "https://access.scooper.co.il/app/project/8408c995-d612-41ed-84ee-69d3c0fa117e/cached/export_Scooper5_x4Kh1K6v.json",
        "youtube_30": "https://access.scooper.co.il/app/project/8408c995-d612-41ed-84ee-69d3c0fa117e/cached/export_Scooper5_fqx1bGPA.json",
        "reddit_30": "https://access.scooper.co.il/app/project/8408c995-d612-41ed-84ee-69d3c0fa117e/cached/export_Scooper5_x4Kh1K6v.json"
    }

    bucket_name = "data-omhds"
    key_prefix = "data/"
    output_key = "scooper_imports"
