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
                if hasattr(user.objects, 'get'):  # Django ORM
                    user_login_obj = user.objects.get(pk=user_login_id)
                else:
                    user_login_obj = user.objects.filter(id=ObjectId(user_login_id)).first()
                print(f"🚀 Direct query SUCCESS: {user_login_obj is not None}")
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
        # ALWAYS print these - they will show in Render logs
        print("="*50)
        print(f"🚀 MIDDLEWARE DEBUG - Path: {request.path}")
        print(f"🚀 MIDDLEWARE DEBUG - Method: {request.method}")
    
    # Skip middleware for login and root
        if request.path in ["/api/loginUser/", "/"]:
            print("🚀 SKIPPING middleware for login/root")
            return self.get_response(request)

    # Print ALL HTTP headers
        http_headers = {k: v for k, v in request.META.items() if k.startswith('HTTP_')}
        print(f"🚀 HTTP Headers: {http_headers}")
    
    # Check specifically for user login header
        user_login_id = request.META.get("HTTP_USER_LOGIN_ID")
        print(f"🚀 USER_LOGIN_ID from header: '{user_login_id}'")
    
    # Check cookies
        cookies = request.COOKIES
        print(f"🚀 Cookies: {cookies}")
    
    # Start with a DRF-style wrapper
        response = createJsonResponse(request)

        try:
            _thread_locals.user_login_id = user_login_id
            print(f"🚀 Set thread local user_login_id: {user_login_id}")

        # Check if user_login_id is None or empty
            if not user_login_id:
                print("❌ ERROR: user_login_id is None or empty!")
                print("❌ This means the HTTP_USER_LOGIN_ID header is missing")
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data["message"] = "Invalid token - missing user header"
                print("❌ Returning 401 - Missing Header")
                return response

        # Attempt to load the user
            print(f"🚀 Looking for user with ObjectId: {user_login_id}")
            try:
                object_id = ObjectId(user_login_id)
                print(f"🚀 Created ObjectId successfully: {object_id}")
            except Exception as oid_error:
                print(f"❌ ERROR creating ObjectId: {oid_error}")
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data["message"] = "Invalid token - bad user ID format"
                return response

            user_login_obj = DatabaseModel.get_document(
            user.objects, {"id": ObjectId(user_login_id)}
        )
            print(f"🚀 DatabaseModel.get_document result: {user_login_obj}")
            print(f"🚀 User found: {user_login_obj is not None}")

            if user_login_obj is not None:
                role = user_login_obj.role or ""
                print(f"🚀 User role: '{role}'")

            # Manage client_id
                client_id = ""
                if role != "superadmin" and getattr(user_login_obj, "client_id", None):
                    client_id = str(user_login_obj.client_id.id)
                _thread_locals.client_id = client_id
                print(f"🚀 Client ID set: '{client_id}'")

            # Check capability
                print(f"🚀 Checking role and capability for role: '{role}'")
                has_capability = check_role_and_capability(request, role)
                print(f"🚀 Has capability: {has_capability}")

                if has_capability:
                    print("✅ User has capability - proceeding to main response")
                    res = self.get_response(request)

                    if isinstance(res, Response):
                        print("🚀 Got DRF Response")
                        response.data["data"] = res.data
                        if isinstance(res.data, dict) and res.data.get("STATUS_CODE") == 401:
                            response.status_code = status.HTTP_401_UNAUTHORIZED
                        response.accepted_renderer = JSONRenderer()
                        response.accepted_media_type = "application/json"
                        response.renderer_context = {}
                        response.render()
                        return response

                    elif isinstance(res, HttpResponseBase):
                        print("🚀 Got Django HttpResponse")
                        return res

                    else:
                        print("🚀 Got plain data response")
                        response.data["data"] = res
                        if isinstance(res, dict) and res.get("STATUS_CODE") == 401:
                            response.status_code = status.HTTP_401_UNAUTHORIZED
                        response.accepted_renderer = JSONRenderer()
                        response.accepted_media_type = "application/json"
                        response.renderer_context = {}
                        response.render()
                        return response
                else:
                    print("❌ ERROR: User does not have capability for this action")
                    response.status_code = status.HTTP_401_UNAUTHORIZED
                    response.data["message"] = "Invalid token - no capability"
            else:
                print("❌ ERROR: user_login_obj is None - user not found in database")
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data["message"] = "Invalid token - user not found"

        except Exception as e:
            print(f"❌ EXCEPTION in middleware: {e}")
            print(f"❌ Exception type: {e.__class__.__name__}")
            import traceback
            print(f"❌ Traceback: {traceback.format_exc()}")
        
            response.data["data"] = False
            if e.__class__.__name__ in ["ExpiredSignatureError", "DecodeError"]:
                response.status_code = status.HTTP_401_UNAUTHORIZED
                response.data["message"] = "Invalid token - JWT error"
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                response.data["message"] = "Internal server error"

    # Default: render & return DRF Response
        print(f"🚀 Final response status: {response.status_code}")
        if isinstance(response, Response):
            response.accepted_renderer = JSONRenderer()
            response.accepted_media_type = "application/json"
            response.renderer_context = {}
            response.render()
    
        print("="*50)
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