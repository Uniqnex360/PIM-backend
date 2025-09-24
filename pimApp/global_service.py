import redis # type: ignore
import pickle
from django.db import models # type: ignore
from mongoengine.queryset import QuerySet   # type: ignore


class DatabaseModel:
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=False)
    @staticmethod
    def get_document(queryset, filter={}, field_list=[]):
        try:
            if isinstance(queryset, QuerySet):
                queryset_name = queryset._document.__name__
                if queryset_name not in ['ProductImage','import_log']:
                    cache_key = f"{queryset_name}:get:{str(filter)}:{str(field_list)}"
                    cached_data = DatabaseModel.redis_client.get(cache_key)
                    if cached_data:
                        return pickle.loads(cached_data)
            data = queryset.filter(**filter).only(*field_list).limit(1)
            if data:
                data = data[0]
                if isinstance(queryset, QuerySet):
                    queryset_name = queryset._document.__name__
                    if queryset_name not in ['ProductImage','import_log']:
                        DatabaseModel.redis_client.setex(cache_key, 3600, pickle.dumps(data))
                return data
            else:
                return None
        except Exception as e:
            print(f"Error occurred while fetching document: {e}")
            return None

    @staticmethod
    def list_documents(queryset, filter={}, field_list=[], sort_list=[], lower_limit=None, upper_limit=None):
        try:
            if isinstance(queryset, QuerySet):
                queryset_name = queryset._document.__name__
                cache_key = f"{queryset_name}:list:{str(filter)}:{str(field_list)}"
                cached_data = DatabaseModel.redis_client.get(cache_key)
                if cached_data:
                    return pickle.loads(cached_data)
            data = queryset(**filter).skip(lower_limit).limit(upper_limit - lower_limit if lower_limit is not None and upper_limit is not None else None).only(*field_list).order_by(*sort_list)
            if data:
                DatabaseModel.redis_client.setex(cache_key, 3600, pickle.dumps(data))
                return data
            else:
                return []  
        except Exception as e:
            print(f"Error occurred while fetching documents: {e}")
            return []

    @staticmethod
    def update_documents(queryset, filter={}, json={}):
        try:
            data = queryset.filter(**filter).update(**json)
            if data:
                if isinstance(queryset, QuerySet):
                    queryset_name = queryset._document.__name__
                    pattern = f"{queryset_name}:*"
                    keys = DatabaseModel.redis_client.keys(pattern)
                    if keys:
                        DatabaseModel.redis_client.delete(*keys)
            data = queryset.filter(**filter).limit(1)
            if data:
                data = data[0]
                return data
            else:
                return None
        except Exception as e:
            print(f"Error occurred while updating documents: {e}")
            return False
    
    def save_documents(queryset,  json={}):
        obj = queryset(**json)
        obj.save()
        return obj

    def delete_documents(queryset,  json={}):
        queryset(**json).delete()
        return True
    
    def count_documents(queryset,filter={}):
        count = queryset(**filter).count()
        return count