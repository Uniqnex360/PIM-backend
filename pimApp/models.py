from django.db import models
from mongoengine import Document,fields,EmbeddedDocument
from bson import ObjectId # type: ignore
from datetime import datetime



class ignore_calls(Document):
    name = fields.StringField()

class client(Document):   #1
    name = fields.StringField(required=True)
    logo = fields.StringField()
    location = fields.StringField()
    website_url = fields.StringField()
    is_active = fields.BooleanField(default=True)
    designation =  fields.StringField()

class user(Document): #2
    name = fields.StringField()
    email = fields.StringField(required=True)
    user_name = fields.StringField(required=True)
    role = fields.StringField()
    password = fields.StringField()
    client_id = fields.ReferenceField(client)
    is_active = fields.BooleanField(default=True)
    last_updated = fields.DateTimeField(default=datetime.now)
    added_by = fields.StringField()
    phone = fields.StringField()

class capability(Document):
    action_name = fields.StringField()
    role_list = fields.ListField(fields.StringField(),default = [])


class email_otp(Document):
    email = fields.EmailField(unique=True)
    otp = fields.StringField()
    expires_at = fields.DateTimeField()


    def __str__(self):
        return f'OTP for {self.email}'
    

    
class attribute_count(Document):
    client_id = fields.ReferenceField(client)
    attribute_count_int = fields.IntField()

class Attribute(Document):
    name = fields.StringField()  
    code = fields.StringField()  
    # attribute_id = fields.StringField()  
    type = fields.StringField(default = 'Text')
    module_name = fields.StringField()
    client_id = fields.ReferenceField(client)
    values = fields.ListField(fields.StringField())
    # product_id = fields.StringField()
    module_id = fields.ListField(fields.StringField(),default = [])
    is_visible = fields.BooleanField(default=True)
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        attribute_count_obj = DatabaseModel.get_document(attribute_count.objects,{'client_id':client_id})
        attribute_number_var = 0
        if attribute_count_obj:
            attribute_count_obj.attribute_count_int += 1
            attribute_number_var = attribute_count_obj.attribute_count_int
            attribute_count_obj.save()
        else:
            DatabaseModel.save_documents(attribute_count,{'client_id':client_id,"attribute_count_int":1})
            attribute_number_var = 1
        self.code = 'ATT-'+'{:04d}'.format(attribute_number_var)
        self.client_id = client_id
        return super(Attribute, self).save(*args, **kwargs)

class Attribute_group_count(Document):
    client_id = fields.ReferenceField(client)
    attribute_group_count_int = fields.IntField()

class Attribute_group(Document):
    name = fields.StringField(required=True)  
    code = fields.StringField()  
    attributes = fields.ListField(fields.ReferenceField(Attribute))  
    Attribute_group_id = fields.StringField() 
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        Attribute_group_count_obj = DatabaseModel.get_document(Attribute_group_count.objects,{'client_id':client_id})
        attribute_number_var = 0
        if Attribute_group_count_obj:
            Attribute_group_count_obj.attribute_group_count_int += 1
            attribute_number_var = Attribute_group_count_obj.attribute_group_count_int
            Attribute_group_count_obj.save()
        else:
            DatabaseModel.save_documents(Attribute_group_count,{'client_id':client_id,"attribute_group_count_int":1})
            attribute_number_var = 1
        self.Attribute_group_id = 'AGP-'+'{:04d}'.format(attribute_number_var)
        self.client_id = client_id
        return super(Attribute_group, self).save(*args, **kwargs)
    

class category_count(Document):
    client_id = fields.ReferenceField(client)
    category_count_int = fields.IntField()
class category(Document):
    category_id = fields.StringField()
    name = fields.StringField()
    client_id = fields.ReferenceField(client)
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        category_count_obj = DatabaseModel.get_document(category_count.objects,{'client_id':client_id})
        category_number_var = 0
        if category_count_obj:
            category_count_obj.category_count_int += 1
            category_number_var = category_count_obj.category_count_int
            category_count_obj.save()
        else:
            DatabaseModel.save_documents(category_count,{'client_id':client_id,"category_count_int":1})
            category_number_var = 1
        if category_number_var < 1000:
            self.category_id = '{:04d}'.format(category_number_var)
        else:
            self.category_id = '{:06d}'.format(category_number_var)
        self.client_id = client_id
        return super(category, self).save(*args, **kwargs)
class category_config_count(Document):
    client_id = fields.ReferenceField(client)
    category_config_count_int = fields.IntField()
class category_config(Document):
    category_config_id = fields.StringField()
    name = fields.StringField()
    levels = fields.ListField(fields.ReferenceField(category))
    attribute_list = fields.ListField(fields.ReferenceField(Attribute),default = list()) 
    client_id = fields.ReferenceField(client)
    end_level = fields.BooleanField()
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())

        category_config_count_obj = DatabaseModel.get_document(category_config_count.objects,{'client_id':client_id})
        category_config_number_var = 0
        if category_config_count_obj:
            category_config_count_obj.category_config_count_int += 1
            category_config_number_var = category_config_count_obj.category_config_count_int
            category_config_count_obj.save()
        else:
            DatabaseModel.save_documents(category_config_count,{'client_id':client_id,"category_config_count_int":1})
            category_config_number_var = 1
        if category_config_number_var < 1000:
            self.category_config_id = '{:04d}'.format(category_config_number_var)
        else:
            self.category_config_id = '{:06d}'.format(category_config_number_var)
        self.client_id = client_id
        return super(category_config, self).save(*args, **kwargs)

class ContactInfo(EmbeddedDocument):
    department_name = fields.StringField()
    email = fields.StringField()
    phone_number = fields.StringField()

class Business_type(Document):
    name = fields.StringField()


class Vendor(Document):
    name = fields.StringField()
    client_id = fields.ReferenceField(client)
    description = fields.StringField()
    business_type = fields.ReferenceField(Business_type)
    address = fields.StringField()
    city = fields.StringField()
    contact_info_email = fields.StringField() 
    contact_info_phone= fields.StringField() 
    logo = fields.StringField()  
    website = fields.StringField()
    tax_info = fields.StringField()
    industry_info = fields.StringField()
    departments = fields.ListField(fields.EmbeddedDocumentField(ContactInfo))
    def save(self, *args, **kwargs):
        from .custom_middleware import get_current_client
        client_id = get_current_client()
        self.client_id = ObjectId(client_id)
        return super(Vendor, self).save(*args, **kwargs)

class brand_count(Document):
    client_id = fields.ReferenceField(client)
    brand_count_int = fields.IntField()


class brand(Document):
    name = fields.StringField()
    brand_id = fields.StringField()
    logo = fields.StringField()  
    country_of_origin = fields.StringField() 
    warranty_details = fields.StringField() 
    warranty_details_based = fields.StringField() 
    status = fields.StringField() 
    website = fields.StringField()  
    client_id = fields.ReferenceField(client)
    description = fields.StringField()
    # vendor_id = fields.ReferenceField(Vendor)
    parent_brand = fields.ReferenceField('self')
    attribute_list = fields.ListField(fields.ReferenceField(Attribute),default = list()) 
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())

        brand_count_obj = DatabaseModel.get_document(brand_count.objects,{'client_id':client_id})
        brand_number_var = 0
        if brand_count_obj:
            brand_count_obj.brand_count_int += 1
            brand_number_var = brand_count_obj.brand_count_int
            brand_count_obj.save()
        else:
            DatabaseModel.save_documents(brand_count,{'client_id':client_id,"brand_count_int":1})
            brand_number_var = 1
        self.brand_id = 'BR-'+'{:04d}'.format(brand_number_var)
        self.client_id = client_id
        return super(brand, self).save(*args, **kwargs)


class Feature(EmbeddedDocument):
    name = fields.StringField()

class Attribute__(EmbeddedDocument):
    name = fields.StringField()
    value = fields.StringField()
class Image(EmbeddedDocument):
    name = fields.StringField()
    url = fields.StringField()
class Attachment(EmbeddedDocument):
    name = fields.StringField()
    url = fields.StringField()
class Video(EmbeddedDocument):
    name = fields.StringField()
    url = fields.StringField()
class RelatedProduct(EmbeddedDocument):
    name = fields.StringField()
    url = fields.StringField()


class ChannelTaxonomy(EmbeddedDocument):
    name = fields.StringField()
    taxonomy_id = fields.StringField()
    category_config_id = fields.ReferenceField(category_config)
    is_active = fields.BooleanField()


class Industry_type(Document):
    name = fields.StringField()

class Manufacture(Document):
    name = fields.StringField()
    client_id = fields.ReferenceField(client)
    def save(self, *args, **kwargs):
        from .custom_middleware import get_current_client
        self.client_id = ObjectId(get_current_client())
        return super(Manufacture, self).save(*args, **kwargs)


# class ProductAttribute(EmbeddedDocument):
#     attribute = fields.ReferenceField(Attribute, required=True) 
#     value = fields.StringField()


class b2c_company(Document):
    name = fields.StringField()

class category_group(Document):
    name = fields.StringField()

class category_group_config(EmbeddedDocument):
    b2c_company_id = fields.ReferenceField(b2c_company)
    category_levels = fields.ListField(fields.ReferenceField(category_group))

class product_count(Document):
    client_id = fields.ReferenceField(client)
    product_count_int = fields.IntField()
class product(Document):
    product_id = fields.StringField()
    mpn = fields.StringField()
    sku = fields.StringField()
    upc = fields.StringField()
    ean = fields.StringField()
    gtin = fields.StringField()
    unspc = fields.StringField()
    model = fields.StringField()
    vendor_id = fields.ReferenceField(Vendor)
    brand_id = fields.ReferenceField(brand)
    manufacture_id = fields.ReferenceField(Manufacture)
    category_id = fields.ListField(fields.ReferenceField(category_config))
    breadcrumb = fields.StringField()
    name = fields.StringField(required=True)
    short_description = fields.StringField()
    personalized_short_description = fields.StringField()
    long_description = fields.StringField()
    personalized_long_description = fields.StringField()
    feature_list = fields.ListField(fields.StringField())
    attribute_list = fields.ListField(fields.ReferenceField(Attribute),default = list()) 
    # attribute_list = fields.ListField(fields.EmbeddedDocumentField(ProductAttribute),default = list())
    related_products = fields.EmbeddedDocumentListField(RelatedProduct)
    category_group_list = fields.ListField(fields.EmbeddedDocumentField(category_group_config),default = list()) 
    application = fields.StringField()
    certifications = fields.StringField()
    Compliance = fields.StringField()
    Prop65 = fields.StringField()
    esg = fields.StringField()
    Hazardous = fields.StringField()
    service_warranty = fields.StringField()
    product_warranty = fields.StringField()
    country_of_origin = fields.StringField()
    currency = fields.StringField()
    msrp = fields.StringField()
    selling_price = fields.StringField()
    discount_price = fields.StringField()
    attachment_list = fields.EmbeddedDocumentListField(Attachment, default=list)
    image_list = fields.EmbeddedDocumentListField(Image, default=list)
    video_list = fields.EmbeddedDocumentListField(Video, default=list)
    client_id = fields.ReferenceField(client)
    def save(self, *args, **kwargs):
        from .global_service import DatabaseModel
        from .custom_middleware import get_current_client
        client_id = ObjectId(get_current_client())
        product_count_obj = DatabaseModel.get_document(product_count.objects,{'client_id':client_id})
        product_number_var = 0
        if product_count_obj:
            product_count_obj.product_count_int += 1
            product_number_var = product_count_obj.product_count_int
            product_count_obj.save()
        else:
            DatabaseModel.save_documents(product_count,{'client_id':client_id,"product_count_int":1})
            product_number_var = 1
        self.product_id = 'BR-'+'{:04d}'.format(product_number_var)
        # self.client_id = client_id
        return super(product, self).save(*args, **kwargs)

class ProductLog(Document):
    product_id = fields.ReferenceField(product)
    action = fields.StringField(choices=['created', 'updated'])  # Log action type
    user = fields.ReferenceField(user)
    timestamp = fields.DateTimeField(default= datetime.utcnow)
class product_category_config(Document):
    category_config_id = fields.ReferenceField(category_config)
    product_id = fields.ReferenceField(product)

class ProductImage(Document):
    name = fields.StringField(required=True, max_length=100)
    image_url = fields.URLField(required=True)  
    original_url = fields.ListField(fields.StringField(),default = [])  
    client_id = fields.ReferenceField(client)
    public_id = fields.StringField()
    
class ProductVideo(Document):
    name = fields.StringField(required=True, max_length=100)
    video_url = fields.URLField(required=True)  
    client_id = fields.ReferenceField(client)
    original_url = fields.ListField(fields.StringField(),default = [])  

    public_id = fields.StringField()

class ProductDocument(Document):
    name = fields.StringField(required=True, max_length=100)
    document_url = fields.URLField(required=True)  
    original_url = fields.ListField(fields.StringField(),default = [])  
    client_id = fields.ReferenceField(client)
    public_id = fields.StringField()


class channelCategory(Document):
    channel_name = fields.StringField()
    taxonomy_level = fields.ListField(fields.StringField())
    category_config_id = fields.ReferenceField(category_config)
    client_id = fields.ReferenceField(client)


class brand_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    action = fields.StringField()
    brand_id = fields.ReferenceField(brand)


class vendor_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    action = fields.StringField()
    vendor_id = fields.ReferenceField(Vendor)


class category_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    action = fields.StringField()
    category_config_id = fields.ReferenceField(category_config)


class product_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    action = fields.StringField()
    product_id = fields.ReferenceField(product)


class attribute_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    action = fields.StringField()
    attribute_id = fields.ReferenceField(Attribute)
    module_name = fields.StringField()


class import_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    module_name = fields.StringField()
    status = fields.StringField(default = "in-progress")
    error_count = fields.IntField(default = 0)
    un_error_count = fields.IntField(default = 0)
    completed_count = fields.IntField(default = 0)
    inprogress_count = fields.IntField(default = 0)
    total_count = fields.IntField(default = 0)
    created_count = fields.IntField(default = 0)
    updated_count = fields.IntField(default = 0)
    created_id_list = fields.ListField(fields.StringField(),default = [])
    updated_id_list = fields.ListField(fields.StringField(),default = [])
    image_count = fields.IntField(default = 0)
    video_count = fields.IntField(default = 0)
    document_count = fields.IntField(default = 0)
    data = fields.DictField()


class export_log(Document):
    user_id = fields.ReferenceField(user)
    client_id = fields.ReferenceField(client)
    logged_date = fields.DateTimeField(default=datetime.now)
    module_name = fields.StringField()
    total_count = fields.IntField(default = 0)
    filter_by = fields.StringField()