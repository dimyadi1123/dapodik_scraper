import scrapy
import json
import csv
import os

class SchoolSpider(scrapy.Spider):
    name = "school_spider"

    def start_requests(self):
        # Membaca daftar npsn dari file CSV
        npsn_list = []
        csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "npsn.csv")
        with open(csv_path, "r") as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                npsn_list.append(row[0])
        
        for npsn in npsn_list:
            yield scrapy.Request(
                url=f"https://dapo.kemdikbud.go.id/api/getHasilPencarian?keyword={npsn}",
                callback=self.parse_first_url,
                meta={"npsn": npsn},
            )

    def parse_first_url(self, response):
        npsn = response.meta["npsn"]
        data = json.loads(response.body)[0]
        nama_sekolah = data.get("nama_sekolah", "")
        sekolah_id_enkrip = data.get("sekolah_id_enkrip", "")
        yield scrapy.Request(
            url=f"https://dapo.kemdikbud.go.id/rekap/sekolahDetail?semester_id=20231&sekolah_id={sekolah_id_enkrip}",
            callback=self.parse_second_url,
            meta={"npsn": npsn, "nama_sekolah": nama_sekolah, "sekolah_id_enkrip": sekolah_id_enkrip},
        )

    def parse_second_url(self, response):
        npsn = response.meta["npsn"]
        nama_sekolah = response.meta["nama_sekolah"]
        sekolah_id_enkrip = response.meta["sekolah_id_enkrip"]
        data_json = json.loads(response.body)[0]

        item = {
            "npsn": npsn,
            "nama_sekolah": nama_sekolah,
            "sekolah_id_enkrip": sekolah_id_enkrip,
            # Masukkan data lain yang ingin diambil dari JSON kedua
            **data_json  # Menambahkan data JSON ke dalam item
        }
        yield item
