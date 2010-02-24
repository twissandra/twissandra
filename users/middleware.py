from lib.database import get_user_by_id, DatabaseError

def get_user(request):
    if 'user_id' in request.session:
        try:
            user = get_user_by_id(request.session['user_id'])
            user['is_authenticated'] = True
        except DatabaseError:
            pass
    return {
        'username': None,
        'password': None,
        'id': None,
        'is_authenticated': False,
    }

class LazyUser(object):
    def __get__(self, request, obj_type=None):
        if not hasattr(request, '_cached_user'):
            request._cached_user = get_user(request)
        return request._cached_user

class UserMiddleware(object):
    def process_request(self, request):
        request.__class__.user = LazyUser()