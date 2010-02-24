import uuid

from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect, Http404
from django.core.urlresolvers import reverse

from tweets.forms import TweetForm

from lib.database import save_tweet, get_timeline, get_userline
from lib.database import get_user_by_username, DatabaseError

NUM_PER_PAGE = 40

def timeline(request):
    form = TweetForm(request.POST or None)
    if request.user['is_authenticated'] and form.is_valid():
        tweet_id = str(uuid.uuid1())
        save_tweet(tweet_id, request.user['id'], {
            'id': tweet_id,
            'user_id': request.user['id'],
            'body': form.cleaned_data['body'],
        })
        return HttpResponseRedirect(reverse('timeline'))
    start = request.GET.get('start')
    tweets = get_timeline(request.user['id'], start=start,
        limit=NUM_PER_PAGE + 1)
    next = None
    if tweets and len(tweets) == NUM_PER_PAGE + 1:
        next = tweets[-1]['_ts']
    tweets = tweets[:NUM_PER_PAGE]
    context = {
        'form': form,
        'tweets': tweets,
        'next': next,
    }
    return render_to_response('tweets/timeline.html', context,
        context_instance=RequestContext(request))

def userline(request, username=None):
    try:
        user = get_user_by_username(username)
    except DatabaseError:
        raise Http404
    start = request.GET.get('start')
    tweets = get_userline(user['id'], start=start, limit=NUM_PER_PAGE + 1)
    next = None
    if tweets and len(tweets) == NUM_PER_PAGE + 1:
        next = tweets[-1]['_ts']
    tweets = tweets[:NUM_PER_PAGE]
    context = {
        'user': user,
        'tweets': tweets,
        'next': next,
    }
    return render_to_response('tweets/userline.html', context,
        context_instance=RequestContext(request))