from django.conf.urls.defaults import *
from django.conf import settings

urlpatterns = patterns('',
    url('^auth/', include('users.urls')),
    url('', include('tweets.urls')),
)

if settings.DEBUG:
    urlpatterns += patterns('django.views.static',
        (r'^media/(?P<path>.*)$', 'serve',
            {'document_root': settings.MEDIA_ROOT, 'show_indexes': True}),
    )
