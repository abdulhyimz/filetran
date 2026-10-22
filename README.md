# filetran

iPad (a-Shell) dahil farklı cihazlar arasında, PairDrop benzeri yerel ağ dosya paylaşımı için Python tabanlı web uygulaması.

## Özellikler
- WebSocket ile anlık aktif kullanıcı listesi
- Cihaz tipi + isim gösterimi ve isim değiştirme
- Karşı tarafa dosya gönderme isteği (kabul/red akışı)
- Modern tek sayfa arayüz (ikon destekli)
- Tek kullanımlık token tabanlı transfer bağlantıları
- Dosya boyutu limiti ve giriş/doğrulama sanitization kontrolleri

## Kurulum

```bash
cd /home/runner/work/filetran/filetran
python -m pip install -r requirements.txt
```

## Çalıştırma

```bash
cd /home/runner/work/filetran/filetran
python app.py --host 0.0.0.0 --port 8765
```

Sonra iPad ve diğer cihazlardan aynı ağda şu adrese girin:

- `http://SUNUCU_IP:8765`

## a-Shell (iPad) notları
- a-Shell içinde Python ile aynı komut çalıştırılabilir.
- Ağdaki diğer cihazların erişebilmesi için iPad'in aynı Wi‑Fi ağında olması gerekir.
- İnternet üzerinden kullanım için ters proxy + TLS (HTTPS/WSS) önerilir.

## Güvenlik ve hız yaklaşımı
- Transferler cihazlar arası onay akışı ile başlar (izinsiz gönderim azaltılır).
- Yükleme/indirme URL’leri tek transfere özel token içerir.
- Sunucu tarafında dosya adı temizleme ve maksimum boyut kontrolü uygulanır.
- WebSocket heartbeat ile kopan bağlantılar hızlı temizlenir.
