from .custom_middleware import createJsonResponse
from .custom_middleware import createCookies
from rest_framework.parsers import JSONParser
from .global_service import DatabaseModel
from rest_framework.decorators import api_view
from django.views.decorators.csrf import csrf_exempt
from django.middleware import csrf
from .models import user
import os
from rest_framework.parsers import JSONParser
import jwt
from rest_framework.decorators import api_view
SIMPLE_JWT=os.getenv('SIMPLE_JWT')
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
@api_view(('GET', 'POST'))
@csrf_exempt
def loginUser(request):
    # Debug and fix SIMPLE_JWT issue
    global SIMPLE_JWT
    print(f"SIMPLE_JWT type: {type(SIMPLE_JWT)}")
    print(f"SIMPLE_JWT value: {repr(SIMPLE_JWT)}")
    
    jsonRequest = JSONParser().parse(request)
    print(f"Login request: {jsonRequest}")
    user_name_or_email=jsonRequest.get('user_name')
    password=jsonRequest.get('password')
    # Transform the request to match your database fields
    query = {
        "$or":[
            {"name":user_name_or_email},
            {"email":user_name_or_email}
            
        ],
        'password':password
    }
    
    print(f"Database query: {query}")
    user_data_obj = DatabaseModel.get_document(user.objects, query)
    print(f"User found: {user_data_obj}")
    
    token = ''
    if user_data_obj == None:
        response = createJsonResponse(request)
        valid = False
    else:
        role_name = user_data_obj.role
        
        # Handle client_id safely
        client_id = ""
        if user_data_obj.role == 'superadmin':
            client_id = ""
        else:
            try:
                if hasattr(user_data_obj, 'client_id') and user_data_obj.client_id:
                    client_id = str(user_data_obj.client_id.id)
                else:
                    client_id = ""
            except (AttributeError, TypeError):
                client_id = ""
        
        payload = {
            'id': str(user_data_obj.id),
            'first_name': user_data_obj.name,
            'email': user_data_obj.email,
            'role_name': role_name.lower().replace(' ', '_'),
            'max_age': SIMPLE_JWT['SESSION_COOKIE_MAX_AGE']
        }
        
        token = jwt.encode(payload=payload, key=SIMPLE_JWT['SIGNING_KEY'], algorithm=SIMPLE_JWT['ALGORITHM'])
        valid = True
        user_data_obj.is_active = True
        response = createJsonResponse(request, token)
        createCookies(token, response)
        response.data['data']['user_login_id'] = str(user_data_obj.id)
        response.data['data']['user_role'] = str(user_data_obj.role)
        response.data['data']['name'] = str(user_data_obj.name)
        response.data['data']['client_id'] = str(client_id)
        csrf.get_token(request)
    
    response.data['data']['valid'] = valid
    return response


@api_view(('GET', 'POST'))
def logout(request):
    response = createJsonResponse(request)
    response.data['data']['status'] = 'logged out'
    return response


import random
import string
from django.core.mail import send_mail
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from datetime import datetime, timedelta
from .models import email_otp 

def generate_otp():
    return ''.join(random.choices(string.digits, k=6))  

@csrf_exempt
def sendOtp(request):
    json_req = JSONParser().parse(request)
    data = dict()
    data['status'] = False
    email = json_req.get('email')
    if not email:
        return  data
    otp = generate_otp()
    email_otp_obj = DatabaseModel.get_document(email_otp.objects,{'email':email})
    if email_otp_obj:
        DatabaseModel.delete_documents(email_otp.objects,{'email':email})
    otp_record = email_otp.objects.create(
        email=email,
        otp=otp,
        expires_at=datetime.now() + timedelta(minutes=5)
    )
    send_mail(
        'Your OTP for password reset',
        f'Your OTP is: {otp}',
        settings.EMAIL_HOST_USER,
        [email],
        fail_silently=False,
    )
    data['status'] = True
    return JsonResponse(data,safe=False)

@csrf_exempt

def resetPassword(request):
    json_req = JSONParser().parse(request)
    otp = json_req.get('otp')
    email = json_req.get('email')
    new_password = json_req.get('newPassword')
    otp_record = DatabaseModel.get_document(email_otp.objects,{'email':email,'otp':otp})
    if otp_record:
        if datetime.now() > otp_record.expires_at:
            return JsonResponse({'error': 'OTP has expired'},safe=True)
        otp_record.delete()
        DatabaseModel.update_documents(user.objects,{'email':email},{'password':new_password})
        return JsonResponse({'success': 'Password updated successfully'}, safe=True)
    return JsonResponse({'error': 'Invalid OTP'},safe=True)