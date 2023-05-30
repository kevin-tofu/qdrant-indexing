docker run --rm -d -p 6333:6333 \
    --name qdrant \
    -v $(pwd)/qdrant_storage:/qdrant/storage \
    qdrant/qdrant
