from rest_framework.response import Response # type: ignore
from django.http import JsonResponse # type: ignore
from .global_service import DatabaseModel
from .models import ignore_calls,capability,user
import os
SIMPLE_JWT=os.getenv('SIMPLE_JWT')
import jwt # type: ignore
from rest_framework import status # type: ignore
from rest_framework.renderers import JSONRenderer # type: ignore

import json

# Parse SIMPLE_JWT string from env into Python dict
if isinstance(SIMPLE_JWT, str):
    try:
        SIMPLE_JWT = json.loads(SIMPLE_JWT)
    except json.JSONDecodeError as e:
        print(f"Failed to parse SIMPLE_JWT env var: {e}")
        # Fallbacks - so you never crash, even if env is not set properly
        SIMPLE_JWT = {
            'SESSION_COOKIE_MAX_AGE': 86400,
            'AUTH_COOKIE_SECURE': False,
            'AUTH_COOKIE_SAMESITE': 'Lax',
            'SESSION_COOKIE_DOMAIN': None,
            'ACCESS_TOKEN_LIFETIME': 86400,
            'SIGNING_KEY': 'fallback-secret-key',
            'ALGORITHM': 'HS256'
        }

def check_ignore_authentication_for_url(request):
    path = request.path.split("/")
    # try:
    #     action = path[2] 
    # except IndexError:
    #     return False
    result_obj = DatabaseModel.get_document(ignore_calls.objects, {"name__in": path})
    return result_obj is not None  
from django.urls import resolve
def skip_for_paths():
    """
    Decorator for skipping middleware based on path
    """
    def decorator(f):       
        def check_if_health(self, request):
            print(">>>>>>>>>>>>>>>>>>>>")

            if check_ignore_authentication_for_url(request): 
                user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
                _thread_locals.user_login_id = user_login_id
                print(">>>>>>>>>>>>>>>>>>>>")
                user_login_obj = DatabaseModel.get_document(user.objects,{'id':user_login_id})
                if user_login_obj :
                    print(user_login_obj,id)
                    if user_login_obj.role != 'superadmin':
                        _thread_locals.client_id = str(user_login_obj.client_id.id)
                else:
                    if str(request.path) != '/api/loginUser/':
                       return f(self, request) 
                return self.get_response(request)  
            return f(self, request) 
        return check_if_health
    return decorator

def createJsonResponse1(message='success', status=True, data=None):
    """Create a JSON response with a message, status, and additional data."""
    response_data = {
        'data': data,
            'message': message,
            'status': status
    }
    return JsonResponse(response_data, content_type='application/json', status=200)
def createJsonResponse(request, token = None):
    c1 = ''
    if token:
        header,payload1,signature = str(token).split(".")
        c1 = header+'.'+payload1
    else:
        data_map = dict()
        data_map['data'] = dict()
        response = Response(content_type = 'application/json') 
        response.data = data_map
        response.data['emessage'] = 'success'
        response.data['estatus'] = True
        # response.data['_c1'] = c1
        response.status_code = 200
        return response
        c1=request.COOKIES.get('_c1')
    data_map = dict()
    data_map['data'] = dict()
    response = Response(content_type = 'application/json') 
    response.data = data_map
    response.data['emessage'] = 'success'
    response.data['estatus'] = True
    response.data['_c1'] = c1
    response.status_code = 200
    return response


def check_authentication(request):
    token=""
    c1=request.COOKIES.get('_c1')
    c2=request.COOKIES.get('_c2')
    if(c1 and c2):    token = c1+"."+c2
    validationObjJWT = None
    try:
        
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        return validationObjJWT
    except Exception as e:
        return validationObjJWT
    return validationObjJWT


def refresh_cookies(request,response):
    token=""
    c1=request.COOKIES.get('_c1')
    c2=request.COOKIES.get('_c2')
    if(c1 and c2):    token = c1+"."+c2
    createCookies(token, response)


def obtainUserObjFromToken(request):
    token = ""
    c1 = request.COOKIES.get('_c1')
    c2 = request.COOKIES.get('_c2')
    if(c1 and c2):    token = c1 + "." + c2
    validationObjJWT = None
    try:
        validationObjJWT = jwt.decode(token, SIMPLE_JWT['SIGNING_KEY'], algorithms=[SIMPLE_JWT['ALGORITHM']])
        # return validationObjJWT["id"],validationObjJWT["name"],validationObjJWT["email"]
        return validationObjJWT
    except Exception as e:
        return validationObjJWT


def check_role_and_capability(request,role_name):
    path = request.path.split("/")
    action = path[2] if len(path) >=3 else None
    is_accessible = False
    capability_obj = DatabaseModel.get_document(capability.objects, {"action_name":action, "role_list__in" : [role_name]})
    if capability_obj != None:
        is_accessible = True 
    return is_accessible
import threading


_thread_locals = threading.local()

def get_current_user():
    return getattr(_thread_locals, 'user_login_id', None)
def get_current_client():
    return getattr(_thread_locals, 'client_id', None)
class CustomMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
    @skip_for_paths()
    def __call__(self, request):
        if request.path in ["/api/loginUser/", "/"]:
            return self.get_response(request)
        response = createJsonResponse(request)
        try:
            user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
            _thread_locals.user_login_id = user_login_id
            user_login_obj = DatabaseModel.get_document(user.objects,{'id':user_login_id})
            if user_login_obj != None:
                role = user_login_obj.role
                if user_login_obj.role == 'superadmin':
                    # _thread_locals.login_client_id = str("super-admin")
                    client_id = ""
                else:
                    # _thread_locals.login_client_id = str(user_data_obj.client_id.id)
                    client_id = str(user_login_obj.client_id.id)
                _thread_locals.client_id = client_id
                print(role)
                # refresh_cookies(request, response)
                if check_role_and_capability(request, role):
                    res = self.get_response(request)
                    if isinstance(res, Response):
                        response.data['data'] = res.data
                        if isinstance(res.data, dict):
                            if res.data.get('STATUS_CODE') == 401:
                                response.status_code = status.HTTP_401_UNAUTHORIZED
                    else:
                        response.data['data'] = res
                        if isinstance(res, dict):
                            if res.get('STATUS_CODE') == 401:
                                response.status_code = status.HTTP_401_UNAUTHORIZED
                else:
                    response.status_code = status.HTTP_401_UNAUTHORIZED
            else:
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data['message'] = 'Invalid token'
        except Exception as e:
            print("Exception Class --", e.__class__)
            print("Exception Class name --", e.__class__.__name__)
            print("Exception --")
            print(e)
            response.data['data'] = False
            if (e.__class__.__name__ == 'ExpiredSignatureError' or e.__class__.__name__ == 'DecodeError'):
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data['message'] = 'Invalid token'
            elif e.__class__.__name__ == 'ValidationError':
                print(str(e))
                print(e.message)
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        response.accepted_renderer = JSONRenderer()
        response.accepted_media_type = "application/json"
        response.renderer_context = {}
        response.render()
        return response


def createCookies(token, response):
    # Fix SIMPLE_JWT if it's a string (same as in loginUser)
    global SIMPLE_JWT
    if isinstance(SIMPLE_JWT, str):
        try:
            import json
            SIMPLE_JWT = json.loads(SIMPLE_JWT)
        except json.JSONDecodeError as e:
            print(f"createCookies: Failed to parse SIMPLE_JWT: {e}")
            # Use hardcoded values as fallback
            SIMPLE_JWT = {
                'SESSION_COOKIE_MAX_AGE': 86400,
                'AUTH_COOKIE_SECURE': False,
                'AUTH_COOKIE_SAMESITE': 'Lax',
                'SESSION_COOKIE_DOMAIN': None,
                'ACCESS_TOKEN_LIFETIME': 86400,
                'SIGNING_KEY': 'fallback-secret-key',
                'ALGORITHM': 'HS256'
            }
    
    header, payload, signature = str(token).split(".")
    response.set_cookie(
        key="_c1",
        value=header + "." + payload,
        max_age=SIMPLE_JWT['SESSION_COOKIE_MAX_AGE'],
        secure=SIMPLE_JWT['AUTH_COOKIE_SECURE'],
        httponly=False,
        samesite=SIMPLE_JWT['AUTH_COOKIE_SAMESITE'],
        domain=SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )
    response.set_cookie(
        key="_c2",
        value=signature,
        expires=SIMPLE_JWT['ACCESS_TOKEN_LIFETIME'],
        secure=SIMPLE_JWT['AUTH_COOKIE_SECURE'],
        httponly=True,
        samesite=SIMPLE_JWT['AUTH_COOKIE_SAMESITE'],
        domain=SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )