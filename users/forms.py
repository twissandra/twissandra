import uuid

from django import forms

import cass

class LoginForm(forms.Form):
    username = forms.CharField(max_length=30)
    password = forms.CharField(widget=forms.PasswordInput(render_value=False))
    
    def clean(self):
        username = self.cleaned_data['username']
        password = self.cleaned_data['password']
        try:
            user = cass.get_user_by_username(username)
        except cass.DatabaseError:
            raise forms.ValidationError(u'Invalid username and/or password')
        if user.get('password') != password:
            raise forms.ValidationError(u'Invalid username and/or password')
        return self.cleaned_data

    def get_username(self):
        return self.cleaned_data['username']
    

class RegistrationForm(forms.Form):
    username = forms.RegexField(regex=r'^\w+$', max_length=30)
    password1 = forms.CharField(widget=forms.PasswordInput(render_value=False))
    password2 = forms.CharField(widget=forms.PasswordInput(render_value=False))
    
    def clean_username(self):
        username = self.cleaned_data['username']
        try:
            cass.get_user_by_username(username)
            raise forms.ValidationError(u'Username is already taken')
        except cass.DatabaseError:
            pass
        return username
    
    def clean(self):
        if ('password1' in self.cleaned_data and 'password2' in 
            self.cleaned_data):
            password1 = self.cleaned_data['password1']
            password2 = self.cleaned_data['password2']
            if password1 != password2:
                raise forms.ValidationError(
                    u'You must type the same password each time')
        return self.cleaned_data
    
    def save(self):
        username = self.cleaned_data['username']
        password = self.cleaned_data['password1']
        cass.save_user(username, {
            'password': password,
        })
        return username
