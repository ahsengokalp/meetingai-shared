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

## Not

Bu paket diger tum MeetingAI paketlerinin ortak bagimliligidir. Ayrı repoya tasindiginda once bunun kurulmasi en temiz yol olur.
