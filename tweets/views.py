import uuid

from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect, Http404
from django.core.urlresolvers import reverse

from tweets.forms import TweetForm

import cass

NUM_PER_PAGE = 40

def timeline(request):
    form = TweetForm(request.POST or None)
    if request.user['is_authenticated'] and form.is_valid():
        tweet_id = str(uuid.uuid1())
        cass.save_tweet(tweet_id, request.user['id'], {
            'id': tweet_id,
            'user_id': request.user['id'],
            'body': form.cleaned_data['body'],
        })
        return HttpResponseRedirect(reverse('timeline'))
    start = request.GET.get('start')
    if request.user['is_authenticated']:
        tweets = cass.get_timeline(request.user['id'], start=start,
            limit=NUM_PER_PAGE + 1)
    else:
        tweets = cass.get_userline(cass.PUBLIC_USERLINE_KEY, start=start,
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

def publicline(request):
    start = request.GET.get('start')
    tweets = cass.get_userline(cass.PUBLIC_USERLINE_KEY, start=start,
        limit=NUM_PER_PAGE + 1)
    next = None
    if tweets and len(tweets) == NUM_PER_PAGE + 1:
        next = tweets[-1]['_ts']
    tweets = tweets[:NUM_PER_PAGE]
    context = {
        'tweets': tweets,
        'next': next,
    }
    return render_to_response('tweets/publicline.html', context,
        context_instance=RequestContext(request))

def userline(request, username=None):
    try:
        user = cass.get_user_by_username(username)
    except cass.DatabaseError:
        raise Http404
    
    # Query for the friend ids
    friend_ids = []
    if request.user['is_authenticated']:
        friend_ids = cass.get_friend_ids(request.user['id']) + [request.user['id']]
    
    # Add a property on the user to indicate whether the currently logged-in
    # user is friends with the user
    user['friend'] = user['id'] in friend_ids
    
    start = request.GET.get('start')
    tweets = cass.get_userline(user['id'], start=start, limit=NUM_PER_PAGE + 1)
    next = None
    if tweets and len(tweets) == NUM_PER_PAGE + 1:
        next = tweets[-1]['_ts']
    tweets = tweets[:NUM_PER_PAGE]
    
    context = {
        'user': user,
        'tweets': tweets,
        'next': next,
        'friend_ids': friend_ids,
    }
    return render_to_response('tweets/userline.html', context,
        context_instance=RequestContext(request))