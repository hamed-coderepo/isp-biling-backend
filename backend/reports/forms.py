from django import forms

class FilterForm(forms.Form):
    creators_raw = forms.CharField(
        label='نام ریسلرها (با کاما جدا کنید)',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'input-main',
            'placeholder': "مثال: Sediqi-Passaband, Another-Reseller"
        })
    )

    filter_serial = forms.BooleanField(required=False, initial=False, label='فیلتر شماره مسلسل',
                                       widget=forms.CheckboxInput(attrs={'class': 'custom-control-input'}))
    serial_op = forms.ChoiceField(
        choices=[
            ('NONE', 'بدون شرط'),
            ('=', 'مساوی'),
            ('<', 'کوچکتر'),
            ('>', 'بزرگتر'),
            ('>=', 'بزرگتر مساوی'),
            ('<=', 'کوچکتر مساوی'),
            ('BETWEEN', 'بازه عددی'),
        ],
        required=False,
        label='نوع شرط',
        widget=forms.Select(attrs={'class': 'select-ui'})
    )
    serial_value = forms.IntegerField(
        required=False,
        label='شماره مسلسل (BigQuery)',
        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'مقدار...'})
    )
    serial_min = forms.IntegerField(
        required=False,
        label='از شماره',
        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'از'})
    )
    serial_max = forms.IntegerField(
        required=False,
        label='تا شماره',
        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'تا'})
    )

    sib_serial_op = forms.ChoiceField(
        choices=[
            ('NONE', 'بدون شرط'),
            ('=', 'مساوی'),
            ('<', 'کوچکتر'),
            ('>', 'بزرگتر'),
            ('>=', 'بزرگتر مساوی'),
            ('<=', 'کوچکتر مساوی'),
            ('BETWEEN', 'بازه عددی'),
        ],
        required=False,
        label='نوع شرط',
        widget=forms.Select(attrs={'class': 'select-ui'})
    )
    sib_serial_value = forms.IntegerField(
        required=False,
        label='شماره مسلسل سیب',
        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'مقدار...'})
    )
    sib_serial_min = forms.IntegerField(
        required=False,
        label='از شماره',
        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'از'})
    )
    sib_serial_max = forms.IntegerField(
        required=False,
        label='تا شماره',
        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'تا'})
    )

    filter_date = forms.BooleanField(required=False, initial=False, label='فیلتر تاریخ',
                                     widget=forms.CheckboxInput(attrs={'class': 'custom-control-input'}))
    date_op = forms.ChoiceField(
        choices=[
            ('NONE', 'بدون شرط'),
            ('=', 'مساوی'),
            ('<', 'قبل از'),
            ('>', 'بعد از'),
            ('>=', 'بزرگتر مساوی'),
            ('<=', 'کوچکتر مساوی'),
            ('BETWEEN', 'بازه تاریخ'),
            ('EXACT', 'دقیق'),
        ],
        required=False,
        label='نوع شرط تاریخ',
        widget=forms.Select(attrs={'class': 'select-ui'})
    )
    date_value = forms.DateField(
        required=False,
        label='تاریخ',
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'dynamic-input'})
    )
    date_start = forms.DateField(
        required=False,
        label='از تاریخ',
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'dynamic-input'})
    )
    date_end = forms.DateField(
        required=False,
        label='تا تاریخ',
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'dynamic-input'})
    )

    service_status = forms.ChoiceField(
        choices=[
            ('NONE', 'بدون شرط'),
            ('Used', 'Used'),
            ('Cancel', 'Cancel'),
            ('Pending', 'Pending'),
            ('Active', 'Active'),
        ],
        required=False,
        label='وضعیت سرویس',
        widget=forms.Select(attrs={'class': 'select-ui'})
    )


class CreatePackageForm(forms.Form):
    server_name = forms.ChoiceField(label='MariaDB Server')
    reseller_username = forms.CharField(
        required=True,
        label='Reseller Username',
        max_length=64,
        widget=forms.TextInput(attrs={'placeholder': 'Exact reseller username'}),
    )
    user_count = forms.IntegerField(
        required=True,
        label='User Count',
        min_value=1,
        widget=forms.NumberInput(attrs={'placeholder': 'تعداد یوزر'}),
    )
    username_prefix = forms.CharField(
        required=True,
        label='Username Prefix',
        max_length=24,
        widget=forms.TextInput(attrs={'placeholder': 'Prefix'}),
    )
    reseller_id = forms.ChoiceField(
        label='Reseller (Hreseller)',
        widget=forms.HiddenInput(),
        required=False,
    )
    visp_id = forms.ChoiceField(label='VISP (Hvisp)')
    center_id = forms.ChoiceField(label='Center (Hcenter)')
    supporter_id = forms.ChoiceField(label='Supporter (Hsupporter)')
    status_id = forms.ChoiceField(label='Status (Hstatus)')
    service_id = forms.ChoiceField(label='Service (Hservice)')

    def __init__(self, *args, **kwargs):
        choices_map = kwargs.pop('choices_map', {})
        super().__init__(*args, **kwargs)
        self.fields['server_name'].choices = choices_map.get('servers', [])
        self.fields['reseller_id'].choices = choices_map.get('resellers', [])
        self.fields['visp_id'].choices = choices_map.get('visps', [])
        self.fields['center_id'].choices = choices_map.get('centers', [])
        self.fields['supporter_id'].choices = choices_map.get('supporters', [])
        self.fields['supporter_id'].widget = forms.HiddenInput()
        default_supporter_id = choices_map.get('default_supporter_id')
        if default_supporter_id:
            self.fields['supporter_id'].initial = default_supporter_id
        self.fields['status_id'].choices = choices_map.get('statuses', [])
        self.fields['status_id'].widget = forms.HiddenInput()
        default_status_id = choices_map.get('default_status_id')
        if default_status_id:
            self.fields['status_id'].initial = default_status_id
        self.fields['service_id'].choices = choices_map.get('services', [])
