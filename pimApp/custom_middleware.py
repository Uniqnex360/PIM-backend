from rest_framework.response import Response # type: ignore
from django.http import JsonResponse # type: ignore
from .global_service import DatabaseModel
from .models import ignore_calls,capability,user
import os
from bson import ObjectId
SIMPLE_JWT=os.getenv('SIMPLE_JWT')
from django.http.response import HttpResponseBase
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
            'AUTH_COOKIE_SECURE': True,
            'AUTH_COOKIE_SAMESITE': 'None',
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
            # print(request.__dict__)


            if check_ignore_authentication_for_url(request): 
                user_login_id = request.META.get('HTTP_USER_LOGIN_ID')
                print(request.META.keys())
                _thread_locals.user_login_id = user_login_id
                print(">>>>>>>>>>>>>>>>>>>>")
                user_login_obj = DatabaseModel.get_document(user.objects,{'id':ObjectId(user_login_id)})
                print('user',user_login_id)
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
def createJsonResponse(request, token=None):
    c1 = ''

    # Handle token splitting
    if token:
        header, payload1, signature = str(token).split(".")
        c1 = header + '.' + payload1
    else:
        c1 = request.COOKIES.get('_c1', '')

    # Build the response payload
    data_map = {
        'data': {},
        'emessage': 'success',
        'estatus': True
    }
    if c1:
        data_map['_c1'] = c1

    # ✅ Pass data to Response at creation time
    response = Response(data_map, status=200, content_type='application/json')

    # ✅ Force renderer context (since middleware created this, not DRF view)
    response.accepted_renderer = JSONRenderer()
    response.accepted_media_type = "application/json"
    response.renderer_context = {}

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
        # Skip middleware for login and root
        if request.path in ["/api/loginUser/", "/"]:
            return self.get_response(request)

        # Start with a DRF-style wrapper
        response = createJsonResponse(request)

        try:
            user_login_id = request.META.get("HTTP_USER_LOGIN_ID")
            _thread_locals.user_login_id = user_login_id

            # Attempt to load the user
            user_login_obj = DatabaseModel.get_document(
                user.objects, {"id": ObjectId(user_login_id)}
            )
            print(f"user_login_obj = {user_login_obj}")

            if user_login_obj is not None:
                role = user_login_obj.role or ""

                # Manage client_id
                client_id = ""
                if role != "superadmin" and getattr(user_login_obj, "client_id", None):
                    client_id = str(user_login_obj.client_id.id)
                _thread_locals.client_id = client_id

                # Check capability
                if check_role_and_capability(request, role):
                    res = self.get_response(request)

                    if isinstance(res, Response):
                        # ✅ DRF Response, merge cleaned data
                        response.data["data"] = res.data
                        if isinstance(res.data, dict) and res.data.get("STATUS_CODE") == 401:
                            response.status_code = status.HTTP_401_UNAUTHORIZED
                        # Finish the DRF response wrapper
                        response.accepted_renderer = JSONRenderer()
                        response.accepted_media_type = "application/json"
                        response.renderer_context = {}
                        response.render()
                        return response

                    elif isinstance(res, HttpResponseBase):
                        # ✅ Regular Django HttpResponse (JsonResponse, error page, etc.)
                        return res

                    else:
                        # ✅ Plain Python dict or data returned
                        response.data["data"] = res
                        if isinstance(res, dict) and res.get("STATUS_CODE") == 401:
                            response.status_code = status.HTTP_401_UNAUTHORIZED
                        response.accepted_renderer = JSONRenderer()
                        response.accepted_media_type = "application/json"
                        response.renderer_context = {}
                        response.render()
                        return response
                else:
                    response.status_code = status.HTTP_401_UNAUTHORIZED
            else:
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data["message"] = "Invalid token"

        except Exception as e:
            print("Middleware Exception:", e)
            response.data["data"] = False
            if e.__class__.__name__ in ["ExpiredSignatureError", "DecodeError"]:
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data["message"] = "Invalid token"
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

        # ✅ Default: render & return DRF Response
        if isinstance(response, Response):
            response.accepted_renderer = JSONRenderer()
            response.accepted_media_type = "application/json"
            response.renderer_context = {}
            response.render()
        return response

def createCookies(token, response):
    global SIMPLE_JWT
    if isinstance(SIMPLE_JWT, str):
        try:
            SIMPLE_JWT = json.loads(SIMPLE_JWT)
        except json.JSONDecodeError:
            SIMPLE_JWT = {
                'SESSION_COOKIE_MAX_AGE': 86400,
                'AUTH_COOKIE_SECURE': True,         # ✅ force secure
                'AUTH_COOKIE_SAMESITE': 'None',     # ✅ allow cross-site
                'SESSION_COOKIE_DOMAIN': None,
                'ACCESS_TOKEN_LIFETIME': 86400,
                'SIGNING_KEY': 'fallback-secret-key',
                'ALGORITHM': 'HS256'
            }

    header, payload, signature = str(token).split(".")

    # ✅ header+payload cookie (non-HttpOnly, for frontend use if needed)
    response.set_cookie(
        key="_c1",
        value=f"{header}.{payload}",
        max_age=SIMPLE_JWT['SESSION_COOKIE_MAX_AGE'],
        secure=True,
        httponly=False,
        samesite="None",
        domain=SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )

    # ✅ signature cookie (HttpOnly, secure)
    response.set_cookie(
        key="_c2",
        value=signature,
        max_age=SIMPLE_JWT['ACCESS_TOKEN_LIFETIME'],  # ✅ use max_age not expires
        secure=True,
        httponly=True,
        samesite="None",
        domain=SIMPLE_JWT['SESSION_COOKIE_DOMAIN'],
    )