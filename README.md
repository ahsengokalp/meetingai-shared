# meetingai-shared

MeetingAI paketleri arasinda paylasilan ortak katman.

## Icerik

- Env tabanli config contract'i
- Note JSON schema
- TypedDict / repository protocol tanimlari
- PostgreSQL tabanli meeting store ve utility katmani
- SQL migration dosyalari: `migrations/`
- Operasyon scriptleri: `scripts/`

## Dis bagimliliklar

- `psycopg[binary]`
- `python-dotenv`

## Kullanim

`meetingai-shared` tek basina calistirilan bir servis degildir. `meetingai-api`, `meetingai-transcription-worker` ve `meetingai-note-worker` tarafindan ortak paket olarak kullanilir.

Lokal kurulum icin:

1. Sanal ortami hazirla:
   `python -m venv .venv`
2. Paketi editable olarak kur:
   `.\.venv\Scripts\python -m pip install -e .`

Yan yana duran servis repolarindan lokal gelisim icin:
`.\.venv\Scripts\python -m pip install -e ..\meetingai_shared`

## Not

Bu paket diger tum MeetingAI paketlerinin ortak bagimliligidir. Ayrı repoya tasindiginda once bunun kurulmasi en temiz yol olur.
