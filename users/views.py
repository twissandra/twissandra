from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect

from users.forms import LoginForm, RegistrationForm

import cass

def login(request):
    login_form = LoginForm()
    register_form = RegistrationForm()
    next = request.REQUEST.get('next')
    if 'kind' in request.POST:
        if request.POST['kind'] == 'login':
            login_form = LoginForm(request.POST)
            if login_form.is_valid():
                user_id = login_form.get_user_id()
                request.session['user_id'] = user_id
                if next:
                    return HttpResponseRedirect(next)
                return HttpResponseRedirect('/')
        elif request.POST['kind'] == 'register':
            register_form = RegistrationForm(request.POST)
            if register_form.is_valid():
                user_id = register_form.save()
                request.session['user_id'] = user_id
                if next:
                    return HttpResponseRedirect(next)
                return HttpResponseRedirect('/')
    context = {
        'login_form': login_form,
        'register_form': register_form,
        'next': next,
    }
    return render_to_response('users/login.html', context,
        context_instance=RequestContext(request))

def logout(request):
    request.session.pop('user_id', None)
    return render_to_response('users/logout.html', {},
        context_instance=RequestContext(request))

def find_friends(request):
    friend_ids = []
    if request.user['is_authenticated']:
        friend_ids = cass.get_friend_ids(request.user['id']) + [
            request.user['id']]
    q = request.GET.get('q')
    result = None
    searched = False
    if q is not None:
        searched = True
        try:
            result = cass.get_user_by_username(q)
            result['friend'] = result['id'] in friend_ids
        except cass.DatabaseError:
            pass
    context = {
        'q': q,
        'result': result,
        'searched': searched,
        'friend_ids': friend_ids,
    }
    return render_to_response('users/add_friends.html', context,
        context_instance=RequestContext(request))

def modify_friend(request):
    next = request.REQUEST.get('next')
    added = False
    removed = False
    if request.user['is_authenticated']:
        if 'add-friend' in request.POST:
            cass.add_friends(request.user['id'], [request.POST['add-friend']])
            added = True
        if 'remove-friend' in request.POST:
            cass.remove_friends(request.user['id'], [request.POST['remove-friend']])
            removed = True
    if next:
        return HttpResponseRedirect(next)
    context = {
        'added': added,
        'removed': removed,
    }
    return render_to_response('users/modify_friend.html', context,
        context_instance=RequestContext(request))