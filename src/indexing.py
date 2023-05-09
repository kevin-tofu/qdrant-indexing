

import argparse
import pandas as pd
import ast


# def create_index()

def main(
    args: argparse.Namespace
):
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams
    from qdrant_client.http.models import CollectionStatus
    from qdrant_client.http.models import PointStruct
    from qdrant_client.http.models import UpdateStatus


    df = pd.read_csv(args.pandas_path)
    point_list = list()
    # For each document creates a JSON document including both text and related vector.
    for _index, row in df.iterrows():

        row_id = row['id']
        row_embedding = ast.literal_eval(row['embedding'])
        # print(type(row_id), row_id)
        # print(type(row_embedding[0]))

        a_point = PointStruct(
            id=row_id,
            vector=row_embedding,
            # payload={"city": "Berlin"}
        )
        point_list.append(a_point)

    dims = len(row_embedding)
    # print(dims)
    qdrant_client = QdrantClient(
        host=args.qdrant_url,
        port=args.qdrant_port,
        prefer_grpc=False
    )

    qdrant_client.recreate_collection(
        collection_name=args.qdarant_collection,
        vectors_config=VectorParams(
            size=dims, 
            distance=Distance.COSINE
        )
    )

    collection_info = qdrant_client.get_collection(
        collection_name=args.qdarant_collection
    )

    assert collection_info.status == CollectionStatus.GREEN
    # assert collection_info.vectors_count == 0


    operation_info = qdrant_client.upsert(
        collection_name=args.qdarant_collection,
        wait=True,
        points=point_list
    )
    # points=[
    #     PointStruct(id=1, vector=[0.05, 0.61, 0.76, 0.74], payload={"city": "Berlin"}),
    #     PointStruct(id=2, vector=[0.19, 0.81, 0.75, 0.11], payload={"city": ["Berlin", "London"]}),
    #     PointStruct(id=3, vector=[0.36, 0.55, 0.47, 0.94], payload={"city": ["Berlin", "Moscow"]}),
    #     PointStruct(id=4, vector=[0.18, 0.01, 0.85, 0.80], payload={"city": ["London", "Moscow"]}),
    #     PointStruct(id=5, vector=[0.24, 0.18, 0.22, 0.44], payload={"count": [0]}),
    #     PointStruct(id=6, vector=[0.35, 0.08, 0.11, 0.44]),
    # ]

    assert operation_info.status == UpdateStatus.COMPLETED



if __name__ == "__main__":
    

    parser = argparse.ArgumentParser()
    parser.add_argument('--qdrant_url', '-QU', type=str, default='localhost', help='')
    parser.add_argument('--qdrant_port', '-QP', type=int, default=6333, help='')
    parser.add_argument('--qdrant_collection', '-QC', type=str, default='localhost', help='')
    parser.add_argument('--pandas_path', '-PP', type=str, default='./dataset.csv', help='')
    args = parser.parse_args()

    main(args)
