.PHONY: build up down logs run install clean

build:
	docker compose build

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

run:
	docker compose exec instagram-uploader python seo_to_instagram.py

install:
	pip install -r requriments.txt
	pip install supabase python-dotenv requests pytrends

clean:
	docker compose down --rmi all --volumes --remove-orphans
