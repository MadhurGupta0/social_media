.PHONY: setup run docker-build docker-run docker-clean

setup:
	python3 -m venv venv
	. venv/bin/activate && pip3 install -r requirements.txt

run:
	. venv/bin/activate && python3 seo_to_instagram.py

docker-clean:
	docker stop socialmedia || true
	docker rm socialmedia || true
	docker rmi socialmedia || true

docker-build: docker-clean
	docker build -t socialmedia .

docker-run: docker-build
	docker run socialmedia
docker-logs:
	docker logs -f socialmedia
