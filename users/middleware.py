import cass

def get_user(request):
    if 'username' in request.session:
        try:
            user = cass.get_user_by_username(request.session['username'])
            user['is_authenticated'] = True
            return user
        except cass.DatabaseError:
            pass
    return {
        'password': None,
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
